package engine

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// waitFlushed blocks until the WAL flusher has written the first count async records.
func waitFlushed(t *testing.T, q *Queue, count uint64) {
	t.Helper()
	require.Eventually(t, func() bool {
		q.w.mu.Lock()
		defer q.w.mu.Unlock()
		return q.w.asyncNext >= count
	}, 5*time.Second, time.Millisecond)
}

// TestCacheGatedOnFirstRead pins the gating: a queue with no reader yet keeps
// an empty ring cache; after the first read, later appends populate it.
func TestCacheGatedOnFirstRead(t *testing.T) {
	q := openAsyncQueue(t, "gated", func(c *Config) { c.GateCache = true })

	for i := range 3 {
		_, err := q.Append(t.Context(), []byte{byte(i)})
		require.NoError(t, err)
	}
	waitFlushed(t, q, 3)

	cur, _ := q.cache.stats()
	require.Zero(t, cur, "a queue with no reader must not populate the ring cache")

	_, err := q.Read(t.Context(), 0)
	require.NoError(t, err)
	require.True(t, q.hasReader.Load())

	_, err = q.Append(t.Context(), []byte("after"))
	require.NoError(t, err)
	cur, _ = q.cache.stats()
	require.Positive(t, cur, "after the first read, appends must populate the ring cache")
}

// TestCachePayloadAliasesEncodedRecord pins the zero-copy cache: reads are
// served from one shared backing array and carry the appended bytes.
func TestCachePayloadAliasesEncodedRecord(t *testing.T) {
	q := openAsyncQueue(t, "alias", func(c *Config) { c.GateCache = true })

	// Activate the reader gate before the record under test is appended.
	for range 2 {
		_, err := q.Append(t.Context(), []byte("warm"))
		require.NoError(t, err)
	}
	waitFlushed(t, q, 2)
	_, err := q.Read(t.Context(), 0)
	require.NoError(t, err)

	const payload = "zero-copy-payload"
	off, err := q.Append(t.Context(), []byte(payload))
	require.NoError(t, err)

	first, err := q.Read(t.Context(), off)
	require.NoError(t, err)
	second, err := q.Read(t.Context(), off)
	require.NoError(t, err)

	assert.Equal(t, payload, string(first.Payload))
	assert.Equal(t, payload, string(second.Payload))
	require.Equal(t, reflect.ValueOf(first.Payload).Pointer(), reflect.ValueOf(second.Payload).Pointer(),
		"repeated reads must share the cached payload slice, not copy it")
}
