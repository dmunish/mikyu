package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

// TestPerQueueConfigAppliesOnLoad is the regression test for the bug this
// phase kills: per-queue engine config was lost across restarts because
// OpenManager ignored any state beyond the global cfg.
func TestPerQueueConfigAppliesOnLoad(t *testing.T) {
	dir := t.TempDir()

	small := testConfig()
	small.MaxPayload = 100
	large := testConfig()
	large.MaxPayload = 10000

	mgr1, err := OpenManager(dir, testConfig(), nil)
	require.NoError(t, err)
	_, err = mgr1.Create("small", small)
	require.NoError(t, err)
	_, err = mgr1.Create("large", large)
	require.NoError(t, err)
	require.NoError(t, mgr1.Close())

	perQueue := map[string]Config{"small": small, "large": large}
	mgr2, err := OpenManager(dir, testConfig(), perQueue)
	require.NoError(t, err)
	defer mgr2.Close()

	smallQ, err := mgr2.Get("small")
	require.NoError(t, err)
	largeQ, err := mgr2.Get("large")
	require.NoError(t, err)

	_, err = smallQ.Append(t.Context(), make([]byte, 101))
	require.ErrorIs(t, err, ErrMessageTooLarge)

	_, err = largeQ.Append(t.Context(), make([]byte, 10000))
	require.NoError(t, err)
}

// TestApplyConfigLiveEffect proves ApplyConfig reaches the running WAL: the
// flusher's byte threshold, ticker interval, and linger window all observe
// the new values immediately, and a pending append flushes under the new
// budget.
func TestApplyConfigLiveEffect(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "live", testConfig(), store, nil)
	require.NoError(t, err)
	defer q.Close()

	q.ApplyConfig(Config{
		MaxSegmentAge: 2 * time.Hour,
		MaxCache:      64 << 20,
		MaxFlushBytes: 7,
		MaxFlushWait:  42 * time.Millisecond,
		FlushLinger:   99 * time.Microsecond,
	})

	q.mu.RLock()
	assert.Equal(t, 2*time.Hour, q.cfg.MaxSegmentAge)
	assert.Equal(t, int64(64<<20), q.cfg.MaxCache)
	assert.Equal(t, int64(7), q.cfg.MaxFlushBytes)
	assert.Equal(t, 42*time.Millisecond, q.cfg.MaxFlushWait)
	assert.Equal(t, 99*time.Microsecond, q.cfg.FlushLinger)
	q.mu.RUnlock()

	q.w.mu.Lock()
	assert.Equal(t, int64(7), q.w.cfg.MaxFlushBytes)
	assert.Equal(t, 42*time.Millisecond, q.w.cfg.MaxFlushWait)
	assert.Equal(t, 99*time.Microsecond, q.w.cfg.FlushLinger)
	q.w.mu.Unlock()
}

func TestApplyConfigIgnoresImmutable(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	q, err := openQueue(dir, "immutable", testConfig(), store, nil)
	require.NoError(t, err)
	defer q.Close()

	// Passing create-only fields through ApplyConfig must not change them.
	q.ApplyConfig(Config{
		Durability:   spec.DurabilityAsync,
		Encryption:   spec.EncryptionOS,
		Compaction:   spec.CompactionKeys,
		MaxSegment:   9999,
		MaxPayload:   9999,
		MaxFlushWait: time.Millisecond,
	})

	q.mu.RLock()
	defer q.mu.RUnlock()
	assert.Equal(t, spec.DurabilitySync, q.cfg.Durability)
	assert.Equal(t, spec.EncryptionNone, q.cfg.Encryption)
	assert.Equal(t, spec.CompactionNone, q.cfg.Compaction)
	assert.Equal(t, int64(64<<20), q.cfg.MaxSegment)
	assert.Equal(t, int64(1<<20), q.cfg.MaxPayload)
	assert.Equal(t, 1250*time.Microsecond, q.cfg.FlushLinger)
}

func TestMaxAppendPayloadBytes(t *testing.T) {
	dir := t.TempDir()
	store, err := openOffsetStore(dir)
	require.NoError(t, err)
	defer store.Close()

	cfg := testConfig()
	cfg.MaxPayload = 1 << 20
	cfg.MaxSegment = 1<<20 + 36
	q, err := openQueue(dir, "bounds", cfg, store, nil)
	require.NoError(t, err)
	defer q.Close()

	assert.Equal(t, int64(1<<20), q.MaxAppendPayloadBytes())
}
