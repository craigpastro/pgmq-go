package pgmq

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/craigpastro/retrier"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const vtDefault = 30

var ErrNoRows = errors.New("pgmq: no rows in result set")

type Message struct {
	MsgID      int64     `json:"msg_id"`
	ReadCount  int64     `json:"read_ct"`
	EnqueuedAt time.Time `json:"enqueued_at"`
	/// VT is "visibility time". The UTC timestamp at which the message will be available for reading again.
	VT      time.Time      `json:"vt"`
	Message map[string]any `json:"message"`
}

type PGMQ struct {
	pool *pgxpool.Pool
}

func New(connString string) (*PGMQ, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating pool: %w", err)
	}

	err = retrier.Do(func() error {
		if err = pool.Ping(context.Background()); err != nil {
			log.Print("waiting for Postgres")
			return err
		}
		return nil
	}, retrier.NewExponentialBackoff())
	if err != nil {
		return nil, fmt.Errorf("error connecting to Postgres: %w", err)
	}

	_, err = pool.Exec(context.Background(), "create extension if not exists pgmq cascade")
	if err != nil {
		return nil, fmt.Errorf("error creating pgmq extension: %w", err)
	}

	return &PGMQ{
		pool: pool,
	}, nil
}

func MustNew(connString string) *PGMQ {
	pgmq, err := New(connString)
	if err != nil {
		panic(err)
	}

	return pgmq
}

// Close closes the underlying connection pool.
func (p *PGMQ) Close() {
	p.pool.Close()
}

// CreateQueue creates a new queue. This sets up the queue's tables, indexes,
// and metadata.
func (p *PGMQ) CreateQueue(ctx context.Context, name string) error {
	_, err := p.pool.Exec(ctx, "select pgmq_create($1)", name)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// Send sends a single message to a queue. The message id, unique to the
// queue, is returned.
func (p *PGMQ) Send(ctx context.Context, queue string, msg map[string]any) (int64, error) {
	var msgID int64
	err := p.pool.QueryRow(ctx, "select * from pgmq_send($1, $2)", queue, msg).Scan(&msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return msgID, nil
}

// Read a single message from the queue. If the queue is empty or all
// messages are invisible, an ErrNoRows errors is returned. If a message is
// returned, it is made invisible for the duration of the visibility timeout
// (vt) in seconds.
func (p *PGMQ) Read(ctx context.Context, queue string, vt int64) (*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	var msg Message
	err := p.pool.
		QueryRow(ctx, "select * from pgmq_read($1, $2, $3)", queue, vt, 1).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// ReadBatch reads a specified number of messages from the queue. Any
// messages that are returned are made invisible for the duration of the
// visibility timeout (vt) in seconds. If vt is 0 it will be set to the
// default value, vtDefault.
//
// If the queue is empty or all messages are invisible an ErrNoRows error is
// returned.
func (p *PGMQ) ReadBatch(ctx context.Context, queue string, vt int64, numMsgs int64) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	rows, err := p.pool.Query(ctx, "select * from pgmq_read($1, $2, $3)", queue, vt, numMsgs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var msgs []*Message
	for rows.Next() {
		var msg Message
		err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
		if err != nil {
			return nil, wrapPostgresError(err)
		}
		msgs = append(msgs, &msg)
	}

	if len(msgs) == 0 {
		return nil, ErrNoRows
	}

	return msgs, nil
}

// Pop reads single message from the queue and deletes it at the same time.
// Similar to Read and ReadBatch if no messages are available an ErrNoRows is
// returned. Unlike these methods, the visibility timeout does not apply.
// This is because the message is immediately deleted.
func (p *PGMQ) Pop(ctx context.Context, queue string) (*Message, error) {
	var msg Message
	err := p.pool.
		QueryRow(ctx, "select * from pgmq_pop($1)", queue).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// Archive moves a message from the queue table to archive table by its id.
// View messages on the archive table with sql:
// ```sql
// SELECT * FROM pgmq_<queue_name>_archive;
// ```
func (p *PGMQ) Archive(ctx context.Context, queue string, msgID int64) (int64, error) {
	tag, err := p.pool.Exec(ctx, "select pgmq_archive($1, $2)", queue, msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return tag.RowsAffected(), nil
}

// Delete deletes a message from the queue by its id. This is a permanent
// delete and cannot be undone. If you want to retain a log of the message,
// use the Archive method.
func (p *PGMQ) Delete(ctx context.Context, queue string, msgID int64) (int64, error) {
	tag, err := p.pool.Exec(ctx, "select pgmq_delete($1, $2)", queue, msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return tag.RowsAffected(), nil
}

func wrapPostgresError(err error) error {
	return fmt.Errorf("postgres error: %w", err)
}
