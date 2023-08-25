package pgfifo

import (
	"errors"
	"fmt"
)

type (
	queueOptions struct {
		TablePrefix           string
		SubscriptionBatchSize uint
	}

	// QueueOption represents a configurable option that can be set for a Queue
	QueueOption struct {
		Name  string
		Value any
	}
)

// setUserOptions sets user-provided options on a Queue
func (q *Queue) setUserOptions(options []QueueOption) error {

	// Set any user-defined options
	for _, option := range options {
		switch option.Name {
		case "TablePrefix":
			val, err := option.Value.(*string)
			if !err {
				return errors.New("TablePrefix option only accepts a string")
			}

			q.options.TablePrefix = *val
		case "SubscriptionBatchSize":
			val, err := option.Value.(*uint)
			if !err {
				return errors.New("SubscriptionSize option only accepts a uint")
			}

			q.options.SubscriptionBatchSize = *val
		default:
			return fmt.Errorf("unknown option specified: %s", option.Name)
		}
	}

	return nil
}

// Return formatted table name for a given table
func (opts *queueOptions) table(t string) string {
	return fmt.Sprintf("%s_%s", opts.TablePrefix, t)
}

// StringOption creates a new QueueOption containing a string
func StringOption(name, value string) QueueOption {
	var option QueueOption

	option.Name = name
	option.Value = &value

	return option
}

// UintOption creates a new QueueOption containing a uint
func UintOption(name string, value uint) QueueOption {
	var option QueueOption

	option.Name = name
	option.Value = &value

	return option
}
