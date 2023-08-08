# A Go (Golang) client for Postgres Message Queue (PGMQ)

A Go (Golang) client for
[Postgres Message Queue](https://github.com/tembo-io/pgmq) (PGMQ). Based on the
[Rust client](https://github.com/tembo-io/pgmq/tree/main/core).

## Usage

Start a Postgres Instance with the PGMQ extension installed:

```shell
docker run -d --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

Then

```go
q, err := MustNew("postgres://postgres:password@127.0.0.1:5432/postgres")
if err != nil {
    // handle error
}

err = q.CreateQueue(ctx, "my_queue")
if err != nil {
    // handle error
}

id, err := q.Send(ctx, "my_queue", map[string]any{"foo": "bar"})
if err != nil {
    // handle error
}

msg, err := q.Read(ctx, "my_queue", 30)
if err != nil {
    // handle error
}

// Archive the message by moving it to the "pgmq_<queue_name>_archive" table.
// Alternatively, you can `Delete` the message, or read and delete in one
// call by using `Pop`.
rowsAffected, err := q.Archive(ctx, "my_queue", id)
if err != nil {
    // handle error
}
```
