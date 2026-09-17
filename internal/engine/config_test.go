package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPerQueueConfigAppliesOnLoad is the regression test for the bug this
// phase kills: per-queue engine config was lost across restarts because
// OpenManager ignored any state beyond the global cfg.
func TestPerQueueConfigAppliesOnLoad(t *testing.T) {
	dir := t.TempDir()

	mgr1, err := OpenManager(dir, Config{}, nil)
	require.NoError(t, err)
	_, err = mgr1.Create("small", Config{MaxMessageBytes: 100})
	require.NoError(t, err)
	_, err = mgr1.Create("large", Config{MaxMessageBytes: 10000})
	require.NoError(t, err)
	require.NoError(t, mgr1.Close())

	perQueue := map[string]Config{
		"small": {MaxMessageBytes: 100},
		"large": {MaxMessageBytes: 10000},
	}
	mgr2, err := OpenManager(dir, Config{}, perQueue)
	require.NoError(t, err)
	defer mgr2.Close()

	small, err := mgr2.Get("small")
	require.NoError(t, err)
	large, err := mgr2.Get("large")
	require.NoError(t, err)

	_, err = small.Append(t.Context(), make([]byte, 101))
	require.ErrorIs(t, err, ErrMessageTooLarge)

	_, err = large.Append(t.Context(), make([]byte, 10000))
	require.NoError(t, err)
}

// TestApplyConfigLiveEffect proves ApplyConfig reaches the running WAL: the
// flusher's batch threshold, ticker interval, and linger window all observe
// the new values immediately, and a pending append flushes under the new
// budget.
func TestApplyConfigLiveEffect(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "live", Config{
		BatchSize:   256,
		MaxInterval: 10 * time.Millisecond,
		MinInterval: 30 * time.Microsecond,
		SegmentAge:  time.Hour,
	}, store)
	require.NoError(t, err)
	defer q.Close()

	q.ApplyConfig(Config{
		BatchSize:   7,
		MaxInterval: 42 * time.Millisecond,
		MinInterval: 99 * time.Microsecond,
		SegmentAge:  2 * time.Hour,
	})

	q.mu.RLock()
	assert.Equal(t, 7, q.cfg.BatchSize)
	assert.Equal(t, 42*time.Millisecond, q.cfg.MaxInterval)
	assert.Equal(t, 99*time.Microsecond, q.cfg.MinInterval)
	assert.Equal(t, 2*time.Hour, q.cfg.SegmentAge)
	q.mu.RUnlock()

	q.w.mu.Lock()
	assert.Equal(t, 7, q.w.cfg.BatchSize)
	assert.Equal(t, 42*time.Millisecond, q.w.cfg.MaxInterval)
	assert.Equal(t, 99*time.Microsecond, q.w.cfg.MinInterval)
	q.w.mu.Unlock()
}

func TestApplyConfigIgnoresImmutable(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "immutable", Config{
		SegmentSize:     1024,
		MaxMessageBytes: 100,
		Encrypted:       false,
		Fsync:           true,
	}, store)
	require.NoError(t, err)
	defer q.Close()

	// Passing create-only fields through ApplyConfig must not change them.
	q.ApplyConfig(Config{
		BatchSize:       7,
		SegmentSize:     9999,
		MaxMessageBytes: 9999,
		Encrypted:       true,
		Fsync:           false,
	})

	q.mu.RLock()
	defer q.mu.RUnlock()
	assert.Equal(t, 7, q.cfg.BatchSize)
	assert.Equal(t, int64(1024), q.cfg.SegmentSize)
	assert.Equal(t, int64(100), q.cfg.MaxMessageBytes)
	assert.False(t, q.cfg.Encrypted)
	assert.True(t, q.cfg.Fsync)
}

// TestApplyConfigNegativeMinIntervalDisablesLinger confirms the documented
// withDefaults semantics survive the live path: a negative MinInterval is
// normalised to 0 (no linger).
func TestApplyConfigNegativeMinIntervalDisablesLinger(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "linger", Config{}, store)
	require.NoError(t, err)
	defer q.Close()

	q.ApplyConfig(Config{MinInterval: -1})
	q.mu.RLock()
	assert.Equal(t, time.Duration(0), q.cfg.MinInterval)
	q.mu.RUnlock()
}
