package pgfifo

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

// Database version
var Version = 1

type queueOptions struct {
	tablePrefix string
}

// Return formatted table name for a given table
func (opts *queueOptions) table(t string) string {
	return fmt.Sprintf("%s_%s", opts.tablePrefix, t)
}

type Queue struct {
	db      *sql.DB
	options queueOptions
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

func (q *Queue) migrate() error {
	// If nothing exists in the database, create it now
	versionTable := q.options.table("version")
	_, err := q.db.Exec(
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
			id 		serial primary key,
			version integer
		);`, versionTable),
	)
	if err != nil {
		return err
	}

	// Next, pull out the version in the database
	var version int
	err = q.db.QueryRow(
		fmt.Sprintf(`SELECT version FROM %s WHERE id = 1`, versionTable),
	).Scan(&version)

	// In this case, our database hasn't been initialized, so initialize it now
	if err == sql.ErrNoRows {
		tx, err := q.db.Begin()
		if err != nil {
			return err
		}

		_, err = tx.Exec(
			fmt.Sprintf("INSERT INTO %s(version) VALUES($1)", versionTable),
			Version,
		)
		if err != nil {
			tx.Rollback()
			return err
		}

		_, err = tx.Exec(
			fmt.Sprintf(`CREATE TABLE %s (
				id 			bigserial primary key,
				queue_time 	timestamptz default now(),
				topic		varchar(256),
				payload 	text
			)`, q.options.table("queue")),
		)
		if err != nil {
			tx.Rollback()
			return err
		}

		err = tx.Commit()
		if err != nil {
			return err
		}

		version = Version
	} else if err != nil {
		return err
	}

	return nil
}
