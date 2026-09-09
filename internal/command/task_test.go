package command_test

import (
	"context"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskCommandLifecycle(t *testing.T) {
	dir := t.TempDir()

	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	defer client.Close()

	ctx := context.Background()

	require.NoError(t, command.Create(ctx, client, "tasks"))

	task, n, err := command.Publish(ctx, client, "tasks", "hello task", command.PublishOptions{})
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, len("hello task"), n)

	popped, err := command.Pop(ctx, client, "tasks", time.Second)
	require.NoError(t, err)
	assert.Equal(t, task.ID, popped.ID)

	require.NoError(t, command.TaskAck(ctx, client, "tasks", popped.ID))
}
