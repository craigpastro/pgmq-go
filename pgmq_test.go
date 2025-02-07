package pgmq

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/craigpastro/pgmq-go/mocks"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

var (
	pg16 = Database{
		image: "quay.io/tembo/pgmq-pg:latest",
	}
	pg17 = Database{
		image: "tembo.docker.scarf.sh/tembo/pg17-pgmq:latest",
	}
	testMsg1 = json.RawMessage(`{"foo": "bar1"}`)
	testMsg2 = json.RawMessage(`{"foo": "bar1"}`)
)

func (d *Database) Close() {
	d.q.Close()
	_ = d.container.Terminate(context.Background())
}

func TestMain(m *testing.M) {
	pg16.Init()
	pg17.Init()
	code := m.Run()
	pg16.Close()
	pg17.Close()
	os.Exit(code)
}

func TestPing(t *testing.T) {
	pg16.TestPing(t)
	pg17.TestPing(t)
}

func TestCreateAndDropQueue(t *testing.T) {
	pg16.TestCreateAndDropQueue(t)
	pg17.TestCreateAndDropQueue(t)
}

func TestDropQueueWhichDoesNotExist(t *testing.T) {
	pg16.TestDropQueueWhichDoesNotExist(t)
	pg17.TestDropQueueWhichDoesNotExist(t)
}

func TestCreateUnloggedAndDropQueue(t *testing.T) {
	pg16.TestCreateUnloggedAndDropQueue(t)
	pg17.TestCreateUnloggedAndDropQueue(t)
}

func TestSend(t *testing.T) {
	pg16.TestSend(t)
	pg17.TestSend(t)
}

func TestSendTimestamp(t *testing.T) {
	//pg16.TestSendWithDelayTimestamp(t) delay with timestamp is not supported in pg16
	pg17.TestSendWithDelayTimestamp(t)
}

func TestSendAMarshalledStruct(t *testing.T) {
	pg16.TestSendAMarshalledStruct(t)
	pg17.TestSendAMarshalledStruct(t)
}

func TestSendInvalidJSONFails(t *testing.T) {
	pg16.TestSendInvalidJSONFails(t)
	pg17.TestSendInvalidJSONFails(t)
}

func TestSendBatch(t *testing.T) {
	pg16.TestSendBatch(t)
	pg17.TestSendBatch(t)
}

func TestSendBatchWithDelayTimestamp(t *testing.T) {
	//pg16.TestSendBatchWithDelayTimestamp(t) delay with timestamp is not supported in pg16
	pg17.TestSendBatchWithDelayTimestamp(t)
}

func TestRead(t *testing.T) {
	pg16.TestRead(t)
	pg17.TestRead(t)
}

func TestReadEmptyQueueReturnsNoRows(t *testing.T) {
	pg16.TestReadEmptyQueueReturnsNoRows(t)
	pg17.TestReadEmptyQueueReturnsNoRows(t)
}

func TestReadBatch(t *testing.T) {
	pg16.TestReadBatch(t)
	pg17.TestReadBatch(t)
}

func TestPop(t *testing.T) {
	pg16.TestPop(t)
	pg17.TestPop(t)
}

func TestPopEmptyQueueReturnsNoRows(t *testing.T) {
	pg16.TestPopEmptyQueueReturnsNoRows(t)
	pg17.TestPopEmptyQueueReturnsNoRows(t)
}

func TestArchive(t *testing.T) {
	pg16.TestArchive(t)
	pg17.TestArchive(t)
}

func TestArchiveNotExist(t *testing.T) {
	pg16.TestArchiveNotExist(t)
	pg17.TestArchiveNotExist(t)
}

func TestArchiveBatch(t *testing.T) {
	pg16.TestArchiveBatch(t)
	pg17.TestArchiveBatch(t)
}

func TestDelete(t *testing.T) {
	pg16.TestDelete(t)
	pg17.TestDelete(t)
}

func TestDeleteNotExist(t *testing.T) {
	pg16.TestDeleteNotExist(t)
	pg17.TestDeleteNotExist(t)
}

func TestDeleteBatch(t *testing.T) {
	pg16.TestDeleteBatch(t)
	pg17.TestDeleteBatch(t)
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

	t.Run("createUnloggedQueueError", func(t *testing.T) {
		mockDB.EXPECT().Exec(ctx, "SELECT pgmq.create_unlogged($1)", queue).Return(cmdTag, testErr)
		err := q.CreateUnloggedQueue(ctx, queue)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("dropQueueError", func(t *testing.T) {
		mockDB.EXPECT().Exec(ctx, "SELECT pgmq.drop_queue($1)", queue).Return(cmdTag, testErr)
		err := q.DropQueue(ctx, queue)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("sendError", func(t *testing.T) {
		mockDB.EXPECT().QueryRow(ctx, "SELECT * FROM pgmq.send($1, $2, $3::int)", queue, gomock.Any(), 0).Return(mockRow)
		mockRow.EXPECT().Scan(gomock.Any()).Return(testErr)
		id, err := q.Send(ctx, queue, testMsg1)
		require.EqualValues(t, 0, id)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("sendBatchError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)", queue, gomock.Any(), 0).Return(nil, testErr)
		ids, err := q.SendBatch(ctx, queue, []json.RawMessage{testMsg1})
		require.Nil(t, ids)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("readError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT * FROM pgmq.read($1, $2, $3)", queue, gomock.Any(), gomock.Any()).Return(nil, testErr)
		msg, err := q.Read(ctx, queue, 0)
		require.Nil(t, msg)
		require.ErrorContains(t, err, "postgres error")
	})

	t.Run("popError", func(t *testing.T) {
		mockDB.EXPECT().Query(ctx, "SELECT * FROM pgmq.pop($1)", queue).Return(nil, testErr)
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
