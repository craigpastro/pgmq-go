# pgmq-go

[![Go Reference](https://pkg.go.dev/badge/github.com/craigpastro/pgmq-go.svg)](https://pkg.go.dev/github.com/craigpastro/pgmq-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/craigpastro/pgmq-go)](https://goreportcard.com/report/github.com/craigpastro/pgmq-go)
[![CI](https://github.com/craigpastro/pgmq-go/actions/workflows/push_to_main.yaml/badge.svg)](https://github.com/craigpastro/pgmq-go/actions/workflows/push_to_main.yaml)
[![codecov](https://codecov.io/github/craigpastro/pgmq-go/branch/main/graph/badge.svg?token=00AJODX77Z)](https://codecov.io/github/craigpastro/pgmq-go)

A Go (Golang) client for
[Postgres Message Queue](https://github.com/tembo-io/pgmq) (PGMQ). Based loosely
on the [Rust client](https://github.com/tembo-io/pgmq/tree/main/core).

## Usage

Start a Postgres instance with the PGMQ extension installed:

```shell
docker run -d --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

Then

```go
package main

import (
    "context"
    "fmt"

    "github.com/craigpastro/pgmq-go"
)

func main() {
    q, err := pgmq.New("postgres://postgres:password@localhost:5432/postgres")
    if err != nil {
        panic(err)
    }

    ctx := context.Background()

    err = q.CreateQueue(ctx, "my_queue")
    if err != nil {
        panic(err)
    }

    id, err := q.Send(ctx, "my_queue", map[string]any{"foo": "bar"})
    if err != nil {
        panic(err)
    }

    msg, err := q.Read(ctx, "my_queue", 30)
    if err != nil {
        panic(err)
    }

    // Archive the message by moving it to the "pgmq_<queue_name>_archive" table.
    // Alternatively, you can `Delete` the message, or read and delete in one
    // call by using `Pop`.
    _, err = q.Archive(ctx, "my_queue", id)
    if err != nil {
        panic(err)
    }
}
```
