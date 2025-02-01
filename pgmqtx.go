package pgmq

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/jackc/pgx/v5"
)

// SendTX starts a transaction and sends a single message to a queue.
// The transaction and the message id, unique to the queue, are returned.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) SendTX(ctx context.Context, queue string, msg json.RawMessage) (pgx.Tx, int64, error) {
	return p.SendWithDelayTX(ctx, queue, msg, 0)
}

// SendWithDelayTX starts a transaction and sends a single message to a queue with a delay.
// The delay is specified in seconds. The transaction and the message id, unique to the queue, are returned.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) SendWithDelayTX(ctx context.Context, queue string, msg json.RawMessage, delay int) (tx pgx.Tx, msgID int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, 0, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, 0, wrapPostgresError(err)
	}

	err = tx.QueryRow(ctx, "SELECT * FROM PGMQ.send($1, $2, $3)", queue, msg, delay).
		Scan(&msgID)
	if err != nil {
		return tx, 0, wrapPostgresError(err)
	}

	return tx, msgID, nil
}

// SendTX starts a transaction and sends a single message to a queue.
// The transaction and the message id, unique to the queue, are returned.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) SendWithTX(ctx context.Context, tx pgx.Tx, queue string, msg json.RawMessage) (int64, error) {
	return p.SendWithTxDelay(ctx, tx, queue, msg, 0)
}

// SendWithTxDelay receives a transaction and sends a single message to a queue with a delay.
// The delay is specified in seconds. The message id, unique to the queue, is returned.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) SendWithTxDelay(ctx context.Context, tx pgx.Tx, queue string, msg json.RawMessage, delay int) (msgID int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return 0, err
	}

	err = tx.
		QueryRow(ctx, "SELECT * FROM PGMQ.send($1, $2, $3)", queue, msg, delay).
		Scan(&msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return msgID, nil
}

// SendBatchTX starts a transaction and sends a batch of messages to a queue.
// The transaction and the message ids, unique to the queue, are returned.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) SendBatchTX(ctx context.Context, queue string, msgs []json.RawMessage) (tx pgx.Tx, msgIDs []int64, err error) {
	return p.SendBatchWithDelayTX(ctx, queue, msgs, 0)
}

// SendBatchWithDelayTX starts a transaction and sends a batch of messages to a queue with a delay.
// The delay is specified in seconds. The transaction and the message ids, unique to the queue, are
// returned.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) SendBatchWithDelayTX(ctx context.Context, queue string, msgs []json.RawMessage, delay int) (tx pgx.Tx, msgIDs []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	rows, err := tx.Query(ctx, "SELECT * FROM PGMQ.send_batch($1, $2::jsonb[], $3)", queue, msgs, delay)
	if err != nil {
		return tx, nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var msgID int64
		err = rows.Scan(&msgID)
		if err != nil {
			return tx, nil, wrapPostgresError(err)
		}
		msgIDs = append(msgIDs, msgID)
	}

	return tx, msgIDs, nil
}

// SendBatchTX receives a transaction and sends a batch of messages to a queue.
// The message ids, unique to the queue, are returned.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) SendBatchWithTX(ctx context.Context, tx pgx.Tx, queue string, msgs []json.RawMessage) (msgIDs []int64, err error) {
	return p.SendBatchWithTXDelay(ctx, tx, queue, msgs, 0)
}

// SendBatchWithDelayTX receives a transaction and sends a batch of messages to a queue with a delay.
// The delay is specified in seconds. The message ids, unique to the queue, are returned.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) SendBatchWithTXDelay(ctx context.Context, tx pgx.Tx, queue string, msgs []json.RawMessage, delay int) (msgIDs []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, "SELECT * FROM PGMQ.send_batch($1, $2::jsonb[], $3)", queue, msgs, delay)
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

// ReadTX creates a transaction and reads a single message from the queue.
// If the queue is empty or all  messages are invisible, an ErrNoRows errors is returned.
// If a message is returned, it is made invisible for the duration of the visibility timeout
// (vt) in seconds.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) ReadTX(ctx context.Context, queue string, vt int64) (tx pgx.Tx, _ *Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	if vt == 0 {
		vt = p.defaultVT
	}

	var msg Message
	err = tx.QueryRow(ctx, "SELECT * FROM PGMQ.read($1, $2, $3)", queue, vt, 1).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tx, nil, ErrNoRows
		}
		return tx, nil, wrapPostgresError(err)
	}

	return tx, &msg, nil
}

// ReadWithTX receives a transaction and reads a single message from the queue.
// If the queue is empty or all messages are invisible, an ErrNoRows errors is returned.
// If a message is returned, it is made invisible for the duration of the visibility timeout
// (vt) in seconds.
// A transaction is received and used.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) ReadWithTX(ctx context.Context, tx pgx.Tx, queue string, vt int64) (_ *Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	if vt == 0 {
		vt = p.defaultVT
	}

	var msg Message
	err = tx.
		QueryRow(ctx, "SELECT * FROM PGMQ.read($1, $2, $3)", queue, vt, 1).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// ReadBatchTX creates a transaction and reads a specified number of messages from the queue.
// Any messages that are returned are made invisible for the duration of the visibility timeout
// (vt) in seconds. If vt is 0 it will be set to the default value (defaultVT).
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) ReadBatchTX(ctx context.Context, queue string, vt int64, numMsgs int64) (tx pgx.Tx, msgs []*Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	if vt == 0 {
		vt = p.defaultVT
	}
	var rows pgx.Rows
	rows, err = tx.Query(ctx, "SELECT * FROM PGMQ.read($1, $2, $3)", queue, vt, numMsgs)
	if err != nil {
		return tx, nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message = Message{}
		err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
		if err != nil {
			return tx, nil, wrapPostgresError(err)
		}
		msgs = append(msgs, &msg)
	}

	return tx, msgs, nil
}

// ReadBatchWithTX receives a transaction and reads a specified number of messages from the queue.
// Any messages that are returned are made invisible for the duration of the visibility timeout
// (vt) in seconds. If vt is 0 it will be set to the default value (defaultVT).
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) ReadBatchWithTX(ctx context.Context, tx pgx.Tx, queue string, vt int64, numMsgs int64) (msgs []*Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	if vt == 0 {
		vt = p.defaultVT
	}
	var rows pgx.Rows
	rows, err = tx.Query(ctx, "SELECT * FROM PGMQ.read($1, $2, $3)", queue, vt, numMsgs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg Message = Message{}
		err := rows.Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
		if err != nil {
			return nil, wrapPostgresError(err)
		}
		msgs = append(msgs, &msg)
	}

	return msgs, nil
}

// PopTX creates a transaction, reads single message from the queue and deletes it at the same time.
// Similar to ReadTX and ReadBatchTX, if no messages are available an ErrNoRows is
// returned. Unlike these methods, the visibility timeout does not apply.
// This is because the message is immediately deleted.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) PopTX(ctx context.Context, queue string) (tx pgx.Tx, _ *Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	var msg Message
	err = tx.QueryRow(ctx, "SELECT * FROM PGMQ.pop($1)", queue).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return tx, nil, ErrNoRows
		}
		return tx, nil, wrapPostgresError(err)
	}

	return tx, &msg, nil
}

// PopWithTX receives a transaction, reads single message from the queue and deletes it at the same time.
// Similar to ReadWithTX and ReadBatchWithTX, if no messages are available an ErrNoRows is
// returned. Unlike these methods, the visibility timeout does not apply.
// This is because the message is immediately deleted.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) PopWithTX(ctx context.Context, tx pgx.Tx, queue string) (_ *Message, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	var msg Message
	err = tx.QueryRow(ctx, "SELECT * FROM PGMQ.pop($1)", queue).
		Scan(&msg.MsgID, &msg.ReadCount, &msg.EnqueuedAt, &msg.VT, &msg.Message)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNoRows
		}
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}

// ArchiveTX creates a transaction and moves a message from the queue table to the archive table
// by its id.
// Messages can be viewed on the archive table with sql:
//
//	SELECT * FROM PGMQ.a_<queue_name>;
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) ArchiveTX(ctx context.Context, queue string, msgID int64) (tx pgx.Tx, archived bool, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, false, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, false, wrapPostgresError(err)
	}

	err = tx.QueryRow(ctx, "SELECT PGMQ.archive($1, $2::bigint)", queue, msgID).
		Scan(&archived)
	if err != nil {
		return tx, false, wrapPostgresError(err)
	}

	return tx, archived, nil
}

// ArchiveWithTX receives a transaction and moves a message from the queue table to the archive table
// by its id.
// Messages can be viewed on the archive table with sql:
//
//	SELECT * FROM PGMQ.a_<queue_name>;
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) ArchiveWithTX(ctx context.Context, tx pgx.Tx, queue string, msgID int64) (archived bool, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return false, err
	}

	err = tx.QueryRow(ctx, "SELECT PGMQ.archive($1, $2::bigint)", queue, msgID).
		Scan(&archived)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return archived, nil
}

// ArchiveBatchTX creates a transaction and moves a batch of messages from the queue table to the archive
// table by their ids.
// Messages can be viewed on the archive table with sql:
//
//	SELECT * FROM PGMQ.a_<queue_name>;
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) ArchiveBatchTX(ctx context.Context, queue string, msgIDs []int64) (tx pgx.Tx, archived []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	rows, err := tx.Query(ctx, "SELECT PGMQ.archive($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return tx, nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return tx, nil, wrapPostgresError(err)
		}
		archived = append(archived, n)
	}

	return tx, archived, nil
}

// ArchiveBatchWithTX receives a transaction and moves a batch of messages from the queue table to the archive
// table by their ids.
// Messages can be viewed on the archive table with sql:
//
//	SELECT * FROM PGMQ.a_<queue_name>;
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) ArchiveBatchWithTX(ctx context.Context, tx pgx.Tx, queue string, msgIDs []int64) (archived []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, "SELECT PGMQ.archive($1, $2::bigint[])", queue, msgIDs)
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

// DeleteTX creates a transaction and deletes a message from the queue by its id.
// This is a permanent delete and cannot be undone.
// If you want to retain a log of the message, use the Archive method.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) DeleteTX(ctx context.Context, queue string, msgID int64) (tx pgx.Tx, deleted bool, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, false, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, false, wrapPostgresError(err)
	}

	err = tx.QueryRow(ctx, "SELECT PGMQ.delete($1, $2::bigint)", queue, msgID).Scan(&deleted)
	if err != nil {
		return tx, false, wrapPostgresError(err)
	}

	return tx, deleted, nil
}

// DeleteTX receives a transaction and deletes a message from the queue by its id.
// This is a permanent delete and cannot be undone.
// If you want to retain a log of the message, use the Archive method.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) DeleteWithTX(ctx context.Context, tx pgx.Tx, queue string, msgID int64) (deleted bool, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return false, err
	}

	err = tx.QueryRow(ctx, "SELECT PGMQ.delete($1, $2::bigint)", queue, msgID).Scan(&deleted)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return deleted, nil
}

// DeleteBatchTX creates a transaction and deletes a batch of messages from the queue by their ids.
// This is a permanent delete and cannot be undone.
// If you want to retain a log of the messages, use the ArchiveBatch method.
//
// Returned transaction will be committed / rolled back by the caller
func (p *PGMQ) DeleteBatchTX(ctx context.Context, queue string, msgIDs []int64) (tx pgx.Tx, deleted []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, nil, err
	}

	tx, err = p.db.BeginTx(p.ctx, p.defaultTxOptions)
	if err != nil {
		return nil, nil, wrapPostgresError(err)
	}

	rows, err := tx.Query(ctx, "SELECT PGMQ.delete($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return tx, nil, wrapPostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var n int64
		if err := rows.Scan(&n); err != nil {
			return tx, nil, wrapPostgresError(err)
		}
		deleted = append(deleted, n)
	}

	return tx, deleted, nil
}

// DeleteBatchWithTX receives a transaction and deletes a batch of messages from the queue by their ids.
// This is a permanent delete and cannot be undone.
// If you want to retain a log of the messages, use the ArchiveBatch method.
//
// Transaction will be committed / rolled back by the caller
func (p *PGMQ) DeleteBatchWithTX(ctx context.Context, tx pgx.Tx, queue string, msgIDs []int64) (deleted []int64, err error) {
	p.ActiveOps.Add(1)
	defer p.ActiveOps.Add(-1)

	// make sure the contexts were not canceled
	if err = errors.Join(p.ctx.Err(), ctx.Err()); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, "SELECT PGMQ.delete($1, $2::bigint[])", queue, msgIDs)
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
