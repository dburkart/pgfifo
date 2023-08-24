package pgfifo

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// Database version
var Version = 1

type (
	queueOptions struct {
		tablePrefix string
	}

	Queue struct {
		db      *sql.DB
		options queueOptions
	}

	Message struct {
		ID        int
		QueueTime time.Time
		Topic     string
		Payload   []byte
	}

	SubscriptionCallback func([]*Message) error
)

// Return formatted table name for a given table
func (opts *queueOptions) table(t string) string {
	return fmt.Sprintf("%s_%s", opts.tablePrefix, t)
}

// Create a new Queue in the specified database
func New(connectionStr string) (*Queue, error) {
	var queue Queue

	db, err := sql.Open("postgres", connectionStr)
	if err != nil {
		return nil, err
	}

	queue.db = db

	// Set defaults
	queue.options.tablePrefix = "pgfifo"

	err = queue.migrate()
	if err != nil {
		return nil, err
	}

	return &queue, err
}

// Publish a message on a particular topic
// We take an interface, and serialize that to the specified topic
func (q *Queue) Publish(topic string, data any) error {
	queueTable := q.options.table("queue")

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
func (q *Queue) Subscribe(topic string, sub SubscriptionCallback) error {
	go func() {
		queueTable := q.options.table("queue")

		// Worker run loop
		for {
			tx, _ := q.db.Begin()

			rows, err := tx.Query(
				fmt.Sprintf(
					`DELETE FROM
						%s
					USING (
						SELECT * FROM %s WHERE topic LIKE '%s%%' LIMIT 10 FOR UPDATE SKIP LOCKED 
					) q
					WHERE q.id = %s.id RETURNING %s.*`,
					queueTable,
					queueTable,
					topic,
					queueTable,
					queueTable,
				),
			)
			if err != nil {
				log.Fatal(err)
			}

			var messages []*Message
			hasNext := rows.Next()
			for hasNext {
				var id int
				var m Message
				err := rows.Scan(&id, &m.QueueTime, &m.Topic, &m.Payload)
				if err != nil {
					log.Fatal(err)
				}
				messages = append(messages, &m)
				hasNext = rows.Next()
			}
			if rows.Err() != nil {
				log.Fatal(rows.Err())
			}

			err = sub(messages)
			if err != nil {
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
