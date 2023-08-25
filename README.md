# pgfifo

Barebones pub/sub message queue built on top of Postgres.

Inspired by pgq and other implementations of this idea around the internet. This is the smallest useful subset (in my opinion) of features for this type of thing.

## Usage

First, get the module:

```shell
go get github.com/dburkart/pgfifo
```

Connect using a connection string supported by pq:

```golang
queue, err := pgfifo.New("postgres://postgres:password@localhost/postgres?sslmode=disable")
if err != nil {
    // Do something with the error
}
```

You can also set options for a given queue instance. For example, you may want to change the default prefix from the default (pgfifo) to "coolapp":

```golang
queue, err := pgfifo.New(
    "postgres://postgres:password@localhost/postgres?sslmode=disable",
    pgfifo.StringOption("TablePrefix", "coolapp"),
)
if err != nil {
    // Do something with the error
}
```

A full list of options can be found in the documentation for `pgfifo.New()`.

Publish an event to a topic:

```golang
queue.Publish("/some/topic", "Data")
```

Create a subscription:

```golang
queue.Subscribe("/topic", func(m []*pgfifo.Message) error {
    for _, m := range m {
        var s string
        m.Decode(&s)
        fmt.Println(m.QueueTime.Format(time.RFC3339), m.Topic, s)
    }
    return nil
})
```