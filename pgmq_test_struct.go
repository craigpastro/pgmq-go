package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/avast/retry-go/v4"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type Database struct {
	q         *PGMQ
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
	d.q, err = retry.DoWithData(func() (*PGMQ, error) {
		return New(ctx, connString)
	})
	if err != nil {
		panic(err)
	}
}

func (d *Database) TestSend(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := d.q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)
	require.EqualValues(t, 1, id)

	id, err = d.q.Send(ctx, queue, testMsg2)
	require.NoError(t, err)
	require.EqualValues(t, 2, id)
}

func (d *Database) TestPing(t *testing.T) {
	err := d.q.Ping(context.Background())
	require.NoError(t, err)
}

func (d *Database) TestCreateAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	err = d.q.DropQueue(ctx, queue)
	require.NoError(t, err)
}

func (d *Database) TestDropQueueWhichDoesNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.DropQueue(ctx, queue)
	require.Error(t, err)
}

func (d *Database) TestCreateUnloggedAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateUnloggedQueue(ctx, queue)
	require.NoError(t, err)

	err = d.q.DropQueue(ctx, queue)
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

	err = d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = d.q.Send(ctx, queue, b)
	require.NoError(t, err)

	msg, err := d.q.Read(ctx, queue, 0)
	require.NoError(t, err)

	var aa A
	err = json.Unmarshal(msg.Message, &aa)
	require.NoError(t, err)

	require.EqualValues(t, a, aa)
}

func (d *Database) TestSendInvalidJSONFails(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = d.q.Send(ctx, queue, json.RawMessage(`{"foo":}`))
	require.Error(t, err)
}

func (d *Database) TestSendBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := d.q.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
}

func (d *Database) TestRead(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := d.q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := d.q.Read(ctx, queue, 0)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	// Visibility timeout will still be in effect.
	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestReadEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestReadBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = d.q.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	time.Sleep(time.Second)
	msgs, err := d.q.ReadBatch(ctx, queue, 0, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	require.Equal(t, testMsg1, msgs[0].Message)
	require.Equal(t, testMsg2, msgs[1].Message)

	// Visibility timeout will still be in effect.
	msgs, err = d.q.ReadBatch(ctx, queue, 0, 5)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

func (d *Database) TestPop(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := d.q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := d.q.Pop(ctx, queue)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestPopEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = d.q.Pop(ctx, queue)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestArchive(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := d.q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	archived, err := d.q.Archive(ctx, queue, id)
	require.NoError(t, err)
	require.True(t, archived)

	// Let's just check that something landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := d.q.db.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestArchiveNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	archived, err := d.q.Archive(ctx, queue, 100)
	require.NoError(t, err)
	require.False(t, archived)
}

func (d *Database) TestArchiveBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := d.q.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	archived, err := d.q.ArchiveBatch(ctx, queue, ids)
	require.NoError(t, err)
	require.Equal(t, ids, archived)

	// Let's check that the two messages landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := d.q.db.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 2, tag.RowsAffected())

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestDelete(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := d.q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	deleted, err := d.q.Delete(ctx, queue, id)
	require.NoError(t, err)
	require.True(t, deleted)

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func (d *Database) TestDeleteNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	deleted, err := d.q.Delete(ctx, queue, 100)
	require.NoError(t, err)
	require.False(t, deleted)
}

func (d *Database) TestDeleteBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := d.q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := d.q.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	deleted, err := d.q.DeleteBatch(ctx, queue, ids)
	require.NoError(t, err)
	require.EqualValues(t, ids, deleted)

	_, err = d.q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}
