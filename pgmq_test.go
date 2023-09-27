package pgmq

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/craigpastro/pgmq-go/mocks"
	"github.com/craigpastro/retrier"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.uber.org/mock/gomock"
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

	q, err = retrier.DoWithData(func() (*PGMQ, error) {
		return New(ctx, connString)
	}, retrier.NewExponentialBackoff())
	if err != nil {
		panic(err)
	}

	code := m.Run()

	q.Close()
	_ = container.Terminate(context.Background())

	os.Exit(code)
}

func TestDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	err = q.DropQueue(ctx, queue)
	require.NoError(t, err)

	_, err = q.Send(ctx, queue, testMsg1)
	require.Error(t, err)
}

func TestDropQueueWhichDoesNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.DropQueue(ctx, queue)
	require.Error(t, err)
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

func TestSendBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := q.SendBatch(ctx, queue, []map[string]any{testMsg1, testMsg2})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)
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

	_, err = q.SendBatch(ctx, queue, []map[string]any{testMsg1, testMsg2})
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

	archived, err := q.Archive(ctx, queue, id)
	require.NoError(t, err)
	require.True(t, archived)

	// Let's just check that something landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := q.db.Exec(ctx, stmt)
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

	archived, err := q.Archive(ctx, queue, 100)
	require.NoError(t, err)
	require.False(t, archived)
}

func TestArchiveBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := q.SendBatch(ctx, queue, []map[string]any{testMsg1, testMsg2})
	require.NoError(t, err)

	archived, err := q.ArchiveBatch(ctx, queue, ids)
	require.NoError(t, err)
	require.Equal(t, ids, archived)

	// Let's check that the two messages landed in the archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := q.db.Exec(ctx, stmt)
	require.NoError(t, err)
	require.EqualValues(t, 2, tag.RowsAffected())

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestDelete(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	id, err := q.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	deleted, err := q.Delete(ctx, queue, id)
	require.NoError(t, err)
	require.True(t, deleted)

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestDeleteNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	deleted, err := q.Delete(ctx, queue, 100)
	require.NoError(t, err)
	require.False(t, deleted)
}

func TestDeleteBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	ids, err := q.SendBatch(ctx, queue, []map[string]any{testMsg1, testMsg2})
	require.NoError(t, err)

	deleted, err := q.DeleteBatch(ctx, queue, ids)
	require.NoError(t, err)
	require.EqualValues(t, ids, deleted)

	_, err = q.Read(ctx, queue, 0)
	require.ErrorIs(t, err, ErrNoRows)
}

func TestErrorCases(t *testing.T) {
	ctx := context.Background()

	queue := t.Name()
	testErr := errors.New("an error")
	cmdTag := pgconn.NewCommandTag("")

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDB := mocks.NewMockDB(ctrl)
	q := PGMQ{db: mockDB}

	mockRow := mocks.NewMockRow(ctrl)

	t.Run("createQueueError", func(t *testing.T) {
		mockDB.EXPECT().Exec(ctx, "SELECT pgmq.create($1)", queue).Return(cmdTag, testErr)
		err := q.CreateQueue(ctx, queue)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("dropQueueError", func(t *testing.T) {
		mockDB.EXPECT().Exec(ctx, "SELECT pgmq.drop_queue($1)", queue).Return(cmdTag, testErr)
		err := q.DropQueue(ctx, queue)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("sendError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3)", queue, gomock.Any(), 0).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		id, err := q.Send(ctx, queue, testMsg1)
		require.EqualValues(t, 0, id)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("sendBatchError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3)", queue, gomock.Any(), 0).Return(nil, testErr)
		ids, err := q.SendBatch(ctx, queue, []map[string]any{testMsg1})
		require.Nil(t, ids)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("readError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, gomock.Any(), gomock.Any()).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		msg, err := q.Read(ctx, queue, 0)
		require.Nil(t, msg)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("popError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT * FROM pgmq.pop($1)", queue).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		msg, err := q.Pop(ctx, queue)
		require.Nil(t, msg)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("archiveError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT pgmq.archive($1, $2::bigint)", queue, gomock.Any()).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		archived, err := q.Archive(ctx, queue, 7)
		require.False(t, archived)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("archiveBatchError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT pgmq.archive($1, $2::bigint[])", queue, gomock.Any()).Return(nil, testErr)
		archived, err := q.ArchiveBatch(ctx, queue, []int64{7})
		require.Nil(t, archived)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("deleteError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT pgmq.delete($1, $2::bigint)", queue, gomock.Any()).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		deleted, err := q.Delete(ctx, queue, 7)
		require.False(t, deleted)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("deleteBatchError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT pgmq.delete($1, $2::bigint[])", queue, gomock.Any()).Return(nil, testErr)
		deleted, err := q.DeleteBatch(ctx, queue, []int64{7})
		require.Nil(t, deleted)
		require.ErrorContains(t, err, "postgres error")
	})
}
