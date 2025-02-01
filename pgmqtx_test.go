package pgmq_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testMsg3 = json.RawMessage(`{"foo": "bar3"}`)
	testMsg4 = json.RawMessage(`{"foo": "bar4"}`)
)

func TestSendTX(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	tx, id, err := q.SendTX(ctx, queue, testMsg1)
	require.NoError(t, err)
	require.EqualValues(t, 1, id)

	id, err = q.SendWithTX(ctx, tx, queue, testMsg2)
	require.NoError(t, err)
	require.EqualValues(t, 2, id)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}

func TestSendBatchTX(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := q.CreateQueue(ctx, queue)
	require.NoError(t, err)

	tx, ids, err := q.SendBatchTX(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2}, ids)

	ids, err = q.SendBatchWithTX(ctx, tx, queue, []json.RawMessage{testMsg3, testMsg4})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, ids)

	tx.Commit(ctx)
	require.NoError(t, err)

	msg, err := q.Read(ctx, queue, 0)
	require.NoError(t, err)
	require.Equal(t, testMsg1, msg.Message)
}
