package pgfifo

// QueueOption represents a configurable option that can be set for a Queue
type QueueOption struct {
	Name  string
	Value any
}

// StringOption creates a new QueueOption containing a string
func StringOption(name, value string) QueueOption {
	var option QueueOption

	option.Name = name
	option.Value = &value

	return option
}
