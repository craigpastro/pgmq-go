package pgmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const vtDefault = 30

var ErrNoRows = errors.New("pgmq: no rows in result set")

type Message struct {
	MsgID      int64
	ReadCount  int64
	EnqueuedAt time.Time
	// VT is "visibility time". The UTC timestamp at which the message will
	// be available for reading again.
	VT      time.Time
	Message json.RawMessage
	Headers json.RawMessage // Only supported in pgmq-pg17 and above
}

type DB interface {
	Ping(ctx context.Context) error
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Close()
}

type PGMQ struct {
	db DB
}

// New uses the connString to attempt to establish a connection to Postgres.
// Once a connetion is established it will create the PGMQ extension if it
// does not already exist.
func New(ctx context.Context, connString string) (*PGMQ, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("error creating pool: %w", err)
	}

	return NewFromDB(ctx, pool)
}

// NewFromDB is a bring your own DB version of New. Given an implementation
// of DB, it will call Ping to ensure the connection has been established,
// then create the PGMQ extension if it does not already exist.
func NewFromDB(ctx context.Context, db DB) (*PGMQ, error) {
	if err := db.Ping(ctx); err != nil {
		return nil, err
	}

	_, err := db.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE")
	if err != nil {
		return nil, fmt.Errorf("error creating pgmq extension: %w", err)
	}

	return &PGMQ{
		db: db,
	}, nil
}

// Close closes the underlying connection pool.
func (p *PGMQ) Close() {
	p.db.Close()
}

// Ping calls the underlying Ping function of the DB interface.
func (p *PGMQ) Ping(ctx context.Context) error {
	return p.db.Ping(ctx)
}

// CreateQueue creates a new queue. This sets up the queue's tables, indexes,
// and metadata.
func (p *PGMQ) CreateQueue(ctx context.Context, queue string) error {
	_, err := p.db.Exec(ctx, "SELECT pgmq.create($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// CreateUnloggedQueue creates a new unlogged queue, which uses an unlogged
// table under the hood. This sets up the queue's tables, indexes, and
// metadata.
func (p *PGMQ) CreateUnloggedQueue(ctx context.Context, queue string) error {
	_, err := p.db.Exec(ctx, "SELECT pgmq.create_unlogged($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// DropQueue deletes the given queue. It deletes the queue's tables, indices,
// and metadata. It will return an error if the queue does not exist.
func (p *PGMQ) DropQueue(ctx context.Context, queue string) error {
	_, err := p.db.Exec(ctx, "SELECT pgmq.drop_queue($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// Send sends a single message to a queue. The message id, unique to the
// queue, is returned.
func (p *PGMQ) Send(ctx context.Context, queue string, msg json.RawMessage) (int64, error) {
	return p.SendWithDelay(ctx, queue, msg, 0)
}

// SendWithDelay sends a single message to a queue with a delay. The delay
// is specified in seconds. The message id, unique to the queue, is returned.
func (p *PGMQ) SendWithDelay(ctx context.Context, queue string, msg json.RawMessage, delay int) (int64, error) {
	var msgID int64
	err := p.db.
		QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3::int)", queue, msg, delay).
		Scan(&msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return msgID, nil
}

// SendWithDelayTimestamp sends a single message to a queue with a delay. The
// delay is specified as a timestamp. The message id, unique to the queue, is
// returned. Only supported in pgmq-pg17 and above.
func (p *PGMQ) SendWithDelayTimestamp(ctx context.Context, queue string, msg json.RawMessage, delay time.Time) (int64, error) {
	var msgID int64
	err := p.db.
		QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3::timestamptz)", queue, msg, delay).
		Scan(&msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return msgID, nil
}

// SendBatch sends a batch of messages to a queue. The message ids, unique to
// the queue, are returned.
func (p *PGMQ) SendBatch(ctx context.Context, queue string, msgs []json.RawMessage) ([]int64, error) {
	return p.SendBatchWithDelay(ctx, queue, msgs, 0)
}

// SendBatchWithDelay sends a batch of messages to a queue with a delay. The
// delay is specified in seconds. The message ids, unique to the queue, are
// returned. Only supported in pgmq-pg17 and above.
func (p *PGMQ) SendBatchWithDelay(ctx context.Context, queue string, msgs []json.RawMessage, delay int) ([]int64, error) {
	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)", queue, msgs, delay)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var msgIDs []int64
	for rows.Next() {
		var msgID int64
		err = rows.Scan(&msgID)
		if err != nil {
			return nil, wrapPostgresError(err)
		}
		msgIDs = append(msgIDs, msgID)
	}

	return msgIDs, nil
}

// SendBatchWithDelayTimestamp sends a batch of messages to a queue with a
// delay. The delay is specified as a timestamp. The message ids, unique to
// the queue, are returned.
func (p *PGMQ) SendBatchWithDelayTimestamp(ctx context.Context, queue string, msgs []json.RawMessage, delay time.Time) ([]int64, error) {
	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::timestamptz)", queue, msgs, delay)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var msgIDs []int64
	for rows.Next() {
		var msgID int64
		err = rows.Scan(&msgID)
		if err != nil {
			return nil, wrapPostgresError(err)
		}
		msgIDs = append(msgIDs, msgID)
	}

	return msgIDs, nil
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
	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, vt, 1)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	fields := rows.FieldDescriptions()
	if len(fields) == 5 {
		err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	} else {
		err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
	}

	if err != nil {
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// ReadBatch reads a specified number of messages from the queue. Any
// messages that are returned are made invisible for the duration of the
// visibility timeout (vt) in seconds. If vt is 0 it will be set to the
// default value, vtDefault.
func (p *PGMQ) ReadBatch(ctx context.Context, queue string, vt int64, numMsgs int64) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, vt, numMsgs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var msgs []*Message
	fields := rows.FieldDescriptions()
	hasHeaders := len(fields) > 5

	for rows.Next() {
		var msg Message
		if hasHeaders {
			err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
		} else {
			err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
		}
		if err != nil {
			return nil, wrapPostgresError(err)
		}
		msgs = append(msgs, &msg)
	}

	return msgs, nil
}

// Pop reads single message from the queue and deletes it at the same time.
// Similar to Read and ReadBatch if no messages are available an ErrNoRows is
// returned. Unlike these methods, the visibility timeout does not apply.
// This is because the message is immediately deleted.
func (p *PGMQ) Pop(ctx context.Context, queue string) (*Message, error) {
	var msg Message
	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.pop($1)", queue)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	fields := rows.FieldDescriptions()
	if len(fields) == 5 {
		err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	} else {
		err = rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message, &msg.Headers)
	}

	if err != nil {
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// Archive moves a message from the queue table to the archive table by its
// id. View messages on the archive table with sql:
//
//	SELECT * FROM pgmq.a_<queue_name>;
func (p *PGMQ) Archive(ctx context.Context, queue string, msgID int64) (bool, error) {
	var archived bool
	err := p.db.QueryRow(ctx, "SELECT pgmq.archive($1, $2::bigint)", queue, msgID).Scan(&archived)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return archived, nil
}

// ArchiveBatch moves a batch of messages from the queue table to the archive
// table by their ids. View messages on the archive table with sql:
//
//	SELECT * FROM pgmq.a_<queue_name>;
func (p *PGMQ) ArchiveBatch(ctx context.Context, queue string, msgIDs []int64) ([]int64, error) {
	rows, err := p.db.Query(ctx, "SELECT pgmq.archive($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var archived []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return nil, wrapPostgresError(err)
		}
		archived = append(archived, n)
	}

	return archived, nil
}

// Delete deletes a message from the queue by its id. This is a permanent
// delete and cannot be undone. If you want to retain a log of the message,
// use the Archive method.
func (p *PGMQ) Delete(ctx context.Context, queue string, msgID int64) (bool, error) {
	var deleted bool
	err := p.db.QueryRow(ctx, "SELECT pgmq.delete($1, $2::bigint)", queue, msgID).Scan(&deleted)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return deleted, nil
}

// DeleteBatch deletes a batch of messages from the queue by their ids. This
// is a permanent delete and cannot be undone. If you want to retain a log of
// the messages, use the ArchiveBatch method.
func (p *PGMQ) DeleteBatch(ctx context.Context, queue string, msgIDs []int64) ([]int64, error) {
	rows, err := p.db.Query(ctx, "SELECT pgmq.delete($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var deleted []int64
	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return nil, wrapPostgresError(err)
		}
		deleted = append(deleted, n)
	}

	return deleted, nil
}

func wrapPostgresError(err error) error {
	return fmt.Errorf("postgres error: %w", err)
}
