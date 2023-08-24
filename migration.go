package pgfifo

import (
	"database/sql"
	"fmt"
)

// Migrate associated queue tables to our current version
// If queue tables don't exist, this function will create them.
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

		_, err = tx.Exec(
			fmt.Sprintf(
				`CREATE INDEX %s ON %s (topic)`,
				q.options.table("topic_index"),
				q.options.table("queue"),
			),
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
