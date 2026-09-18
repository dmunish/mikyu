package node

import (
	"sync/atomic"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRegistryWriteFsyncsDirectory asserts creating a queue fsyncs both its segment directory and queues.json.
func TestRegistryWriteFsyncsDirectory(t *testing.T) {
	var count atomic.Int64
	engine.SyncDirHook = func() { count.Add(1) }
	t.Cleanup(func() { engine.SyncDirHook = nil })

	dir := t.TempDir()
	n, release, err := Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	count.Store(0)
	createLog(t, n, "durable", spec.Spec{})
	assert.Equal(t, int64(2), count.Load(), "create must fsync the new segment and the rewritten queues.json")
}