package command_test

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskLifecycle(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
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

func TestParseTaskRefJSON(t *testing.T) {
	json := `{"id":"abc","payload":"x","attempts":1,"max_attempts":3,"offset":42}`
	ref, id := command.ParseTaskRef(json)
	assert.Equal(t, "abc", id)
	task, ok := ref.(mikyu.Task)
	require.True(t, ok)
	assert.Equal(t, uint64(42), task.Offset)

	ref, id = command.ParseTaskRef("bare-id")
	assert.Equal(t, "bare-id", id)
	_, ok = ref.(mikyu.TaskID)
	assert.True(t, ok)
}
