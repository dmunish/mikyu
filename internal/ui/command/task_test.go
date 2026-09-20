package command_test

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskCommandLifecycle(t *testing.T) {
	dir := t.TempDir()

	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	ctx := t.Context()

	require.NoError(t, command.Create(ctx, client, mikyu.QueueKindTask, "tasks"))

	task, n, err := command.Enqueue(ctx, client, "tasks", "hello task", command.EnqueueOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, len("hello task"), n)

	popped, err := command.Dequeue(ctx, client, "tasks", time.Second)
	require.NoError(t, err)
	assert.Equal(t, task.ID, popped.ID)

	require.NoError(t, command.TaskAck(ctx, client, "tasks", mikyu.TaskID(popped.ID)))
}
