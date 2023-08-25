// Package pgfifo implements a barebones Pub/Sub message Queue backed
// by a Postgres database.
package pgfifo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Database version
var Version = 1

// New creates and returns a new Queue in the specified database.
// The provided connection string should conform to a connection string
// acceptable to [github.com/lib/pq].
//
// Available options that can be passed in when creating a Queue:
//
//   - TablePrefix (string) -- namespace to prefix on pgfifo tables. Defaults to "pgfifo"
//   - SubscriptionBatchSize (uint) -- batch size for subscriptions. Defaults to 10
//
// A new Queue is returned if successful, and an error is returned if
// creating a new queue failed for some reason.
func New(connectionStr string, options ...QueueOption) (*Queue, error) {
	var queue Queue

	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, err
	}

	queue.db = db

	// Set defaults
	queue.options.TablePrefix = "pgfifo"
	queue.options.SubscriptionBatchSize = 10

	// Set any user-defined options
	err = queue.setUserOptions(options)
	if err != nil {
		return nil, err
	}

	err = queue.migrate()
	if err != nil {
		return nil, err
	}

	return &queue, err
}

type (
	// Queue object
	Queue struct {
		db      *sql.DB
		options queueOptions
	}

	// A Message represents a single item in the Queue.
	// The Payload of the message is encoded as JSON
	Message struct {
		QueueTime time.Time
		Topic     string
		Payload   []byte
	}

	// SubscriptionCallback is a client-provided callback.
	// When new events are ready to be consumed, they are passed to this function.
	// If an error is returned by this callback, all events are reprocessed.
	SubscriptionCallback func([]*Message) error
)

// Return formatted table name for a given table
func (opts *queueOptions) table(t string) string {
	return fmt.Sprintf("%s_%s", opts.TablePrefix, t)
}

// Publish a message on a particular topic
// We take an interface, and serialize that to the specified topic
func (q *Queue) Publish(topic string, data any) error {
	queueTable := q.options.table("queue")

	// FIXME: Is JSON the best here?
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = q.db.Query(
		fmt.Sprintf(`INSERT INTO %s (topic, payload) VALUES ($1, $2)`, queueTable),
		topic,
		b,
	)

	return err
}

// Subscribe creates an asynchronous subscription to a particular topic
// TODO: We need to figure out how to handle asynchronous errors.
func (q *Queue) Subscribe(topic string, sub SubscriptionCallback) error {
	go func() {
		queueTable := q.options.table("queue")

		// Worker run loop
		for {
			var messages []*Message
			var hasMore bool

			tx, _ := q.db.Begin()

			// Use row-level locking to ensure that multiple clients don't reprocess
			// already processed data
			rows, err := tx.Query(
				fmt.Sprintf(
					`DELETE FROM
						%s
					USING (
						SELECT * FROM %s WHERE topic LIKE '%s%%' LIMIT %d FOR UPDATE SKIP LOCKED 
					) q
					WHERE q.id = %s.id RETURNING %s.*`,
					queueTable,
					queueTable,
					topic,
					q.options.SubscriptionBatchSize,
					queueTable,
					queueTable,
				),
			)
			if err != nil {
				// FIXME: Do something with the error
				tx.Rollback()
				goto next
			}

			// Pull out all results into a slice to send to the provided callback function
			hasMore = rows.Next()
			for hasMore {
				var id int
				var m Message
				err := rows.Scan(&id, &m.QueueTime, &m.Topic, &m.Payload)
				if err != nil {
					// FIXME: Do something with the error
					tx.Rollback()
					goto next
				}
				messages = append(messages, &m)
				hasMore = rows.Next()
			}
			if rows.Err() != nil {
				// FIXME: Do something with the error
				tx.Rollback()
				goto next
			}

			// If an error is returned by the provided callback, we assume the batch needs to
			// be reprocessed.
			err = sub(messages)
			if err != nil {
				// FIXME: Do something with the error
				tx.Rollback()
				goto next
			}

			tx.Commit()

		next:
			time.Sleep(time.Millisecond * 100)
		}

	}()

	return nil
}

// Helper function to decode a message to a source type
func (m *Message) Decode(t any) error {
	return json.Unmarshal(m.Payload, t)
}
