package command_test

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueLifecycle(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	ctx := t.Context()

	require.NoError(t, command.Create(ctx, client, mikyu.QueueKindLog, "events"))

	queues, err := command.List(ctx, client)
	require.NoError(t, err)
	require.Len(t, queues, 1)
	assert.Equal(t, "events", queues[0].Name)
	assert.Equal(t, mikyu.QueueKindLog, queues[0].Kind)

	d, err := command.Describe(ctx, client, "events")
	require.NoError(t, err)
	assert.Equal(t, mikyu.QueueKindLog, d.Kind)

	executed, err := command.Delete(ctx, client, "events", func() (bool, error) { return true, nil })
	require.NoError(t, err)
	assert.True(t, executed)

	queues, _ = command.List(ctx, client)
	assert.Empty(t, queues)
}

func TestDeleteCancelled(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	ctx := t.Context()
	require.NoError(t, command.Create(ctx, client, mikyu.QueueKindLog, "events"))

	executed, err := command.Delete(ctx, client, "events", func() (bool, error) { return false, nil })
	require.NoError(t, err)
	assert.False(t, executed)

	queues, _ := command.List(ctx, client)
	assert.Len(t, queues, 1)
}

func TestTightensRetention(t *testing.T) {
	age := func(v string) *mikyu.Duration {
		d := mikyu.Duration(mustParse(t, v))
		return &d
	}
	assert.True(t, command.TightensRetention(
		mikyu.Spec{Retention: mikyu.RetentionSpec{MaxAge: age("24h")}},
		mikyu.Spec{Retention: mikyu.RetentionSpec{MaxAge: age("1h")}},
	))
	assert.False(t, command.TightensRetention(
		mikyu.Spec{Retention: mikyu.RetentionSpec{MaxAge: age("1h")}},
		mikyu.Spec{Retention: mikyu.RetentionSpec{MaxAge: age("24h")}},
	))
}

func mustParse(t *testing.T, v string) mikyu.Duration {
	t.Helper()
	d, err := time.ParseDuration(v)
	require.NoError(t, err)
	return mikyu.Duration(d)
}
