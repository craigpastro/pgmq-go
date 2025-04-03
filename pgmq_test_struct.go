package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testMsg1 = json.RawMessage(`{"foo": "bar1"}`)
	testMsg2 = json.RawMessage(`{"foo": "bar1"}`)
)

type Database struct {
	Pool      *pgxpool.Pool
	image     string
	container testcontainers.Container
}

func (d *Database) Init() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        d.image,
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "password"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections"),
	}
	var err error
	d.container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}
	host, err := d.container.Host(ctx)
	if err != nil {
		panic(err)
	}
	port, err := d.container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		panic(err)
	}
	connString := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres", host, port.Port())
	d.Pool, err = retry.DoWithData(func() (*pgxpool.Pool, error) {
		pool, err := NewPgxPool(ctx, connString)
		if err != nil {
			return nil, fmt.Errorf("error creating pool: %w", err)
		}

		err = CreatePGMQExtension(ctx, pool)
		if err != nil {
			return nil, err
		}

		return pool, nil
	})
	if err != nil {
		panic(err)
	}
}

func (d *Database) TestSend(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)
	require.EqualValues(t, 1, id)

	id, err = Send(ctx, d.Pool, queue, testMsg2)
	require.NoError(t, err)
	require.EqualValues(t, 2, id)
}

func (d *Database) TestSendWithDelayTimestamp(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := SendWithDelayTimestamp(ctx, d.Pool, queue, testMsg1, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.EqualValues(t, 1, id)
}

func (d *Database) TestPing(t *testing.T) {
	err := d.Pool.Ping(context.Background())
	require.NoError(t, err)
}

func (d *Database) TestCreateAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	err = DropQueue(ctx, d.Pool, queue)
	require.NoError(t, err)
}

func (d *Database) TestDropQueueWhichDoesNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := DropQueue(ctx, d.Pool, queue)
	require.Error(t, err)
}

func (d *Database) TestCreateUnloggedAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateUnloggedQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	err = DropQueue(ctx, d.Pool, queue)
	require.NoError(t, err)
}

func (d *Database) TestSendAMarshalledStruct(t *testing.T) {
	type A struct {
		Val int `json:"val"`
	}

	a := A{3}
	b, err := json.Marshal(a)
	require.NoError(t, err)

	ctx := context.Background()
	queue := t.Name()

	err = CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	_, err = Send(ctx, d.Pool, queue, b)
	require.NoError(t, err)

	msg, err := Read(ctx, d.Pool, queue, 0)
	require.NoError(t, err)

	var aa A
	err = json.Unmarshal(msg.Message, &aa)
	require.NoError(t, err)

	require.EqualValues(t, a, aa)
}

func (d *Database) TestSendInvalidJSONFails(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	_, err = Send(ctx, d.Pool, queue, json.RawMessage(`{"foo":}`))
	require.Error(t, err)
}

func (d *Database) TestSendBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	ids, err := SendBatch(ctx, d.Pool, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
}

func (d *Database) TestSendBatchWithDelayTimestamp(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	ids, err := SendBatchWithDelayTimestamp(ctx, d.Pool, queue, []json.RawMessage{testMsg1, testMsg2}, time.Now().Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
}

func (d *Database) TestRead(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)

	msg, err := Read(ctx, d.Pool, queue, 0)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	// Visibility timeout will still be in effect.
	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestReadEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestReadBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	_, err = SendBatch(ctx, d.Pool, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	time.Sleep(time.Second)
	msgs, err := ReadBatch(ctx, d.Pool, queue, 0, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	require.Equal(t, testMsg1, msgs[0].Message)
	require.Equal(t, testMsg2, msgs[1].Message)

	// Visibility timeout will still be in effect.
	msgs, err = ReadBatch(ctx, d.Pool, queue, 0, 5)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

func (d *Database) TestPop(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)

	msg, err := Pop(ctx, d.Pool, queue)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestPopEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	_, err = Pop(ctx, d.Pool, queue)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestArchive(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)

	archived, err := Archive(ctx, d.Pool, queue, id)
	require.NoError(t, err)
	require.True(t, archived)

	// Let's just check that something landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := d.Pool.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestArchiveNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	archived, err := Archive(ctx, d.Pool, queue, 100)
	require.NoError(t, err)
	require.False(t, archived)
}

func (d *Database) TestArchiveBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	ids, err := SendBatch(ctx, d.Pool, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	archived, err := ArchiveBatch(ctx, d.Pool, queue, ids)
	require.NoError(t, err)
	require.Equal(t, ids, archived)

	// Let's check that the two messages landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := d.Pool.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 2, tag.RowsAffected())

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestDelete(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)

	deleted, err := Delete(ctx, d.Pool, queue, id)
	require.NoError(t, err)
	require.True(t, deleted)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestDeleteNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	deleted, err := Delete(ctx, d.Pool, queue, 100)
	require.NoError(t, err)
	require.False(t, deleted)
}

func (d *Database) TestDeleteBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	ids, err := SendBatch(ctx, d.Pool, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	deleted, err := DeleteBatch(ctx, d.Pool, queue, ids)
	require.NoError(t, err)
	require.EqualValues(t, ids, deleted)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestSetVisibilityTimeout(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := CreateQueue(ctx, d.Pool, queue)
	require.NoError(t, err)

	id, err := Send(ctx, d.Pool, queue, testMsg1)
	require.NoError(t, err)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.NoError(t, err)

	// msg is in visibility timeout, nothing is returned
	_, err = Read(ctx, d.Pool, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)

	// changing visibility timeout to 0, make it available immediately
	_, err = SetVisibilityTimeout(ctx, d.Pool, queue, id, 0)
	require.NoError(t, err)

	_, err = Read(ctx, d.Pool, queue, 0)
	require.NoError(t, err)
}
