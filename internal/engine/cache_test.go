package engine

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

// TestRingCacheByteCap inserts records past the byte budget and asserts the cache stays under it.
func TestRingCacheByteCap(t *testing.T) {
	r := newRingCache(1024)
	for i := range 100 {
		r.Put(Record{Offset: uint64(i), Timestamp: time.Now(), Payload: bytes.Repeat([]byte("x"), 128)})
	}

	r.mu.Lock()
	cur := r.curBytes
	r.mu.Unlock()
	require.LessOrEqual(t, cur, int64(1024))
	require.Greater(t, cur, int64(0), "the newest records must stay cached")

	// The newest record is still a hit; the oldest were evicted FIFO.
	_, ok := r.Get(99)
	require.True(t, ok)
	_, ok = r.Get(0)
	require.False(t, ok)
}

func TestRingCacheEvictionAccountsBytes(t *testing.T) {
	r := newRingCache(1 << 20)
	for i := range 10 {
		r.Put(Record{Offset: uint64(i), Timestamp: time.Now(), Payload: bytes.Repeat([]byte("y"), 100)})
	}

	r.mu.Lock()
	full := r.curBytes
	r.mu.Unlock()
	require.Equal(t, int64(10*(100+spec.RecordOverhead)), full)

	r.EvictBefore(5)
	r.mu.Lock()
	afterFloor := r.curBytes
	r.mu.Unlock()
	require.Equal(t, int64(5*(100+spec.RecordOverhead)), afterFloor)

	r.Evict(7)
	r.mu.Lock()
	afterEvict := r.curBytes
	r.mu.Unlock()
	require.Equal(t, int64(4*(100+spec.RecordOverhead)), afterEvict)
}

func TestRingCacheSlotBounds(t *testing.T) {
	tiny := newRingCache(1)
	assert.Len(t, tiny.slots, minRingCacheSlots)

	small := newRingCache(1024)
	assert.Len(t, small.slots, 64, "1024/256 rounds up to the 64-slot floor")

	huge := newRingCache(1 << 30)
	assert.Len(t, huge.slots, maxRingCacheSlots)
}
