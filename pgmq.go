package pgmq

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Message struct {
	MsgID      int64
	ReadCount  int64
	EnqueuedAt time.Time
	// VT is "visibility time". The UTC timestamp at which the message will
	// be available for reading again.
	VT      time.Time
	Message json.RawMessage
}

type PGMQ struct {
	db      *pgxpool.Pool
	ctx     context.Context
	cancelF context.CancelFunc

	ActiveOps atomic.Int64

	defaultVT        int64
	defaultTxOptions pgx.TxOptions
}

// NewFromPgxConnStr creates a PGMQ object if connString is valid
// and a connection is established with the underlying database
func NewFromPgxConnStr(ctx context.Context, connString string) (pgMQ *PGMQ, pool *pgxpool.Pool, err error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, nil, err
	}

	return NewFromPgxConfig(ctx, cfg)
}

// NewFromPgxConfig creates a PGMQ object if pgConf is valid
// and a connection is established with the underlying database
func NewFromPgxConfig(ctx context.Context, pgConf *pgxpool.Config) (pgMQ *PGMQ, pool *pgxpool.Pool, err error) {
	pool, err = pgxpool.NewWithConfig(ctx, pgConf)
	if err != nil {
		return nil, nil, err
	}

	return NewFromPgxPool(ctx, pool)
}

// NewFromPgxPool creates a PGMQ object if the provided srcpool
// has a valid connection to the underlying database and
// the pgmq extension is available
func NewFromPgxPool(ctx context.Context, srcpool *pgxpool.Pool) (pgMQ *PGMQ, pool *pgxpool.Pool, err error) {
	err = srcpool.Ping(ctx)
	if err != nil {
		return nil, nil, err
	}

	if err = ctx.Err(); err != nil {
		return nil, nil, err
	}
	_, err = srcpool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE;")
	if err != nil {
		return nil, nil, errors.New("error creating pgmq extension: " + err.Error())
	}

	pgMQ = &PGMQ{
		db:        srcpool,
		defaultVT: 30,
	}
	pgMQ.ctx, pgMQ.cancelF = context.WithCancel(ctx)
	pgMQ.ActiveOps.Store(0)

	return pgMQ, pgMQ.db, err
}

// WithDefaultVT will change the default visibility time value
func (p *PGMQ) WithDefaultVT(newVT int64) *PGMQ {
	p.defaultVT = newVT
	return p
}

// WithDefaultTxOptions will change the default TX options
func (p *PGMQ) WithDefaultTxOptions(newTxOpt pgx.TxOptions) *PGMQ {
	p.defaultTxOptions = newTxOpt
	return p
}

// Close closes the underlying connection pool.
// Waits maximum 10s before closing database connection
func (p *PGMQ) Close() {
	p.CloseWithWaitDuration(time.Second * 10)
}

// CloseWithWaitDuration closes the underlying connection pool.
// Waits maximum the provided duration before closing database connection
func (p *PGMQ) CloseWithWaitDuration(d time.Duration) {
	p.cancelF()
	st := time.Now()
	for {
		// wait no more than 10s
		if time.Since(st) > d {
			break
		}
		// wait for ongoing operations to finish
		if p.ActiveOps.Load() > 0 && time.Since(st) < d {
			time.Sleep(time.Second)
			continue
		}
		break
	}
	p.db.Close()
}

// Ping calls the underlying Ping function of the DB interface.
func (p *PGMQ) Ping(ctx context.Context) (err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return err
	}
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
func (p *PGMQ) DropQueue(ctx context.Context, queue string) (err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return err
	}
	_, err = p.db.Exec(ctx, "SELECT pgmq.drop_queue($1)", queue)
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
func (p *PGMQ) SendWithDelay(ctx context.Context, queue string, msg json.RawMessage, delay int) (msgID int64, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return 0, err
	}
	err = p.db.
		QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3)", queue, msg, delay).
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
// returned.
func (p *PGMQ) SendBatchWithDelay(ctx context.Context, queue string, msgs []json.RawMessage, delay int) (msgIDs []int64, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3)", queue, msgs, delay)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

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
func (p *PGMQ) Read(ctx context.Context, queue string, vt int64) (_ *Message, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	if vt == 0 {
		vt = p.defaultVT
	}

	var msg Message
	err = p.db.
		QueryRow(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, vt, 1).
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
func (p *PGMQ) ReadBatch(ctx context.Context, queue string, vt int64, numMsgs int64) (msgs []*Message, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	if vt == 0 {
		vt = p.defaultVT
	}

	rows, err := p.db.Query(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, vt, numMsgs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message
		err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
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
func (p *PGMQ) Pop(ctx context.Context, queue string) (_ *Message, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	var msg Message
	err = p.db.
		QueryRow(ctx, "SELECT * FROM pgmq.pop($1)", queue).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// Archive moves a message from the queue table to the archive table by its
// id. View messages on the archive table with sql:
//
//	SELECT * FROM pgmq.a_<queue_name>;
func (p *PGMQ) Archive(ctx context.Context, queue string, msgID int64) (archived bool, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return false, err
	}
	err = p.db.QueryRow(ctx, "SELECT pgmq.archive($1, $2::bigint)", queue, msgID).Scan(&archived)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return archived, nil
}

// ArchiveBatch moves a batch of messages from the queue table to the archive
// table by their ids. View messages on the archive table with sql:
//
//	SELECT * FROM pgmq.a_<queue_name>;
func (p *PGMQ) ArchiveBatch(ctx context.Context, queue string, msgIDs []int64) (archived []int64, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := p.db.Query(ctx, "SELECT pgmq.archive($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

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
func (p *PGMQ) Delete(ctx context.Context, queue string, msgID int64) (deleted bool, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return false, err
	}
	err = p.db.QueryRow(ctx, "SELECT pgmq.delete($1, $2::bigint)", queue, msgID).Scan(&deleted)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return deleted, nil
}

// DeleteBatch deletes a batch of messages from the queue by their ids. This
// is a permanent delete and cannot be undone. If you want to retain a log of
// the messages, use the ArchiveBatch method.
func (p *PGMQ) DeleteBatch(ctx context.Context, queue string, msgIDs []int64) (deleted []int64, err error) {
	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := p.db.Query(ctx, "SELECT pgmq.delete($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return nil, wrapPostgresError(err)
		}
		deleted = append(deleted, n)
	}

	return deleted, nil
}

func (p *PGMQ) Exec(ctx context.Context, sql string, args ...any) (r pgx.Rows, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	return p.db.Query(ctx, sql, args...)
}
