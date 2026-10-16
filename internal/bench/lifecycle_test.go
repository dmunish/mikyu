package bench

import (
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAcquireRelease(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	release, err := acquire(t.Context(), client, dir, "bench-1", []queueRef{{Name: "q", Kind: KindLog}}, 0, true, nil)
	require.NoError(t, err)

	runs, err := readRuns(dir)
	require.NoError(t, err)
	require.Len(t, runs.Runs, 1)
	assert.Equal(t, "bench-1", runs.Runs[0].RunID)

	require.NoError(t, release())
	runs, err = readRuns(dir)
	require.NoError(t, err)
	assert.Empty(t, runs.Runs)
}

func TestStaleReconcile(t *testing.T) {
	dir := t.TempDir()
	client, err := mikyu.Open(dir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	_, err = client.CreateLog(t.Context(), "stale-q")
	require.NoError(t, err)

	require.NoError(t, writeRuns(dir, benchRuns{Runs: []runEntry{{
		RunID:  "old",
		Queues: []queueRef{{Name: "stale-q", Kind: KindLog}, {Name: "missing-q", Kind: KindLog}},
	}}}))
	require.NoError(t, reconcileStale(t.Context(), client, dir, nil))

	runs, err := readRuns(dir)
	require.NoError(t, err)
	assert.Empty(t, runs.Runs)

	_, err = client.Log(t.Context(), "stale-q")
	require.ErrorIs(t, err, mikyu.ErrQueueNotFound, "the stale queue must be deleted")
}

func TestQueueNames(t *testing.T) {
	assert.Equal(t, []string{"bench-123-0", "bench-123-1", "bench-123-2"}, queueNames("bench-123", 3))
}

func TestGenerateRunID(t *testing.T) {
	a, b := generateRunID(), generateRunID()
	assert.Contains(t, a, "bench-")
	assert.NotEqual(t, a, b)
}
