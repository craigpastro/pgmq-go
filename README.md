# pgmq-go

[![Go Reference](https://pkg.go.dev/badge/github.com/craigpastro/pgmq-go.svg)](https://pkg.go.dev/github.com/craigpastro/pgmq-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/craigpastro/pgmq-go)](https://goreportcard.com/report/github.com/craigpastro/pgmq-go)
[![CI](https://github.com/craigpastro/pgmq-go/actions/workflows/push_to_main.yaml/badge.svg)](https://github.com/craigpastro/pgmq-go/actions/workflows/push_to_main.yaml)
[![codecov](https://codecov.io/github/craigpastro/pgmq-go/branch/main/graph/badge.svg?token=00AJODX77Z)](https://codecov.io/github/craigpastro/pgmq-go)

A Go (Golang) client for
[Postgres Message Queue](https://github.com/tembo-io/pgmq) (PGMQ). Based loosely
on the [Rust client](https://github.com/tembo-io/pgmq/tree/main/pgmq-rs).

`pgmq-go` works with [pgx](https://github.com/jackc/pgx). The second argument of most functions only needs to satisfy the [DB](https://pkg.go.dev/github.com/craigpastro/pgmq-go#DB) interface, which means it can take, among others, a `*pgx.Conn`, `*pgxpool.Pool`, or `pgx.Tx`.

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
    ctx := context.Background()

    pool, err := pgmq.NewPgxPool(ctx, "postgres://postgres:password@localhost:5432/postgres")
    if err != nil {
        panic(err)
    }

    err = pgmq.CreatePGMQExtension(ctx, pool)
    if err != nil {
        panic(err)
    }

    err := pgmq.CreateQueue(ctx, pool, "my_queue")
    if err != nil {
        panic(err)
    }

    // We can perform various queue operations using a transaction.
    tx, err := pool.Begin(ctx)
    if err != nil {
        panic(err)
    }

    id, err := pgmq.Send(ctx, tx, "my_queue", json.RawMessage(`{"foo": "bar"}`))
    if err != nil {
        panic(err)
    }

    msg, err := pgmq.Read(ctx, tx, "my_queue", 30)
    if err != nil {
        panic(err)
    }

    // Archive the message by moving it to the "pgmq.a_<queue_name>" table.
    // Alternatively, you can `Delete` the message, or read and delete in one
    // call by using `Pop`.
    _, err = pgmq.Archive(ctx, tx, "my_queue", id)
    if err != nil {
        panic(err)
    }

    // Commit the transaction.
    err = tx.Commit(ctx)
    if err != nil {
        panic(err)
    }

    // Close the connection pool.
    pool.Close()
}
```

## Contributions

We :heart: contributions.
