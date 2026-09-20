package manager

import (
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Kind is structural: log/events and task/events are distinct queues and Get
// reports the kind the directory declares.
func TestKindByPath(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir, engine.Config{}, nil)
	require.NoError(t, err)
	defer func() { _ = m.Close() }()
	ctx := t.Context()

	_, err = m.Create(ctx, "events", KindLog, engine.Config{})
	require.NoError(t, err)
	_, err = m.Create(ctx, "events", KindTask, engine.Config{})
	require.NoError(t, err)
	_, err = m.Create(ctx, "jobs", KindTask, engine.Config{})
	require.NoError(t, err)

	// Get checks log/ first, then task/.
	_, kind, err := m.Get("events")
	require.NoError(t, err)
	assert.Equal(t, KindLog, kind)

	_, kind, err = m.Get("jobs")
	require.NoError(t, err)
	assert.Equal(t, KindTask, kind)

	assert.DirExists(t, filepath.Join(dir, "log", "events"))
	assert.DirExists(t, filepath.Join(dir, "task", "events"))
	assert.DirExists(t, filepath.Join(dir, "task", "jobs"))

	infos, err := m.List(ctx)
	require.NoError(t, err)
	require.Len(t, infos, 3)
	assert.Equal(t, QueueInfo{Name: "events", Kind: KindLog}, infos[0])
	assert.Equal(t, QueueInfo{Name: "events", Kind: KindTask}, infos[1])
	assert.Equal(t, QueueInfo{Name: "jobs", Kind: KindTask}, infos[2])
}

// GetOf refuses a queue addressed through the wrong driver.
func TestGetOfRefusesWrongKind(t *testing.T) {
	dir := t.TempDir()
	m, err := Open(dir, engine.Config{}, nil)
	require.NoError(t, err)
	defer func() { _ = m.Close() }()
	ctx := t.Context()

	_, err = m.Create(ctx, "jobs", KindTask, engine.Config{})
	require.NoError(t, err)

	_, err = m.GetOf("jobs", KindLog)
	assert.ErrorIs(t, err, ErrWrongKind)

	_, err = m.GetOf("jobs", KindTask)
	require.NoError(t, err)

	_, err = m.GetOf("absent", KindLog)
	assert.ErrorIs(t, err, engine.ErrQueueNotFound)
}