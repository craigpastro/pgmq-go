package pgmq

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var q *PGMQ

var (
	testMsg1 = map[string]any{"foo": "bar1"}
	testMsg2 = map[string]any{"foo": "bar2"}
)

func TestMain(m *testing.M) {
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "quay.io/tembo/pgmq-pg:latest",
		ExposedPorts: []string{"5432/tcp"},
		Env:          map[string]string{"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "password"},
		WaitingFor:   wait.ForLog("database system is ready to accept connections"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		panic(err)
	}

	port, err := container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		panic(err)
	}

	connString := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres", host, port.Port())

	q = MustNew(connString)

	code := m.Run()

	q.Close()
	_ = container.Terminate(context.Background())

	os.Exit(code)
}

func TestSend(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)
	require.EqualValues(t, 1, id)

	id, err = q.Send(ctx, queue, testMsg2)
	require.NoError(t, err)
	require.EqualValues(t, 2, id)
}

func TestRead(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := q.Read(ctx, queue, 0)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	// Visibility timeout will still be in effect.
	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestReadEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestReadBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	_, err = q.Send(ctx, queue, testMsg2)
	require.NoError(t, err)

	time.Sleep(time.Second)
	msgs, err := q.ReadBatch(ctx, queue, 0, 5)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	require.Equal(t, testMsg1, msgs[0].Message)
	require.Equal(t, testMsg2, msgs[1].Message)

	// Visibility timeout will still be in effect.
	_, err = q.ReadBatch(ctx, queue, 0, 5)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestReadBatchEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = q.ReadBatch(ctx, queue, 0, 1)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestPop(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := q.Pop(ctx, queue)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
	require.Equal(t, id, msg.MsgID)

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestPopEmptyQueueReturnsNoRows(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	_, err = q.Pop(ctx, queue)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestArchive(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	rowsAffected, err := q.Archive(ctx, queue, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	// Let's just check that something landing in the archive table.
	stmt := fmt.Sprintf("select * from pgmq_%s_archive", queue)
	tag, err := q.pool.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 1, tag.RowsAffected())

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestArchiveNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	rowsAffected, err := q.Archive(ctx, queue, 100)
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	rowsAffected, err := q.Delete(ctx, queue, id)
	require.NoError(t, err)
	require.EqualValues(t, 1, rowsAffected)

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestDeleteNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	rowsAffected, err := q.Delete(ctx, queue, 100)
	require.NoError(t, err)
	require.EqualValues(t, 0, rowsAffected)
}
