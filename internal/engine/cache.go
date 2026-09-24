package engine

import (
	"sync"

	"github.com/dmunish/mikyu/internal/spec"
)

const (
	minRingCacheSlots    = 64
	maxRingCacheSlots    = 65536
	ringCacheAvgRecBytes = 256
)

type ringCache struct {
	mu       sync.Mutex
	slots    []Record
	mask     uint64
	maxBytes int64
	curBytes int64
	head     uint64 // next offset to evict; advances FIFO as the byte budget is exceeded
	started  bool
}

func newRingCache(maxBytes int64) *ringCache {
	capacity := min(max(nextPow2(maxBytes/ringCacheAvgRecBytes), minRingCacheSlots), maxRingCacheSlots)
	return &ringCache{
		slots:    make([]Record, capacity),
		mask:     capacity - 1,
		maxBytes: maxBytes,
	}
}

func nextPow2(n int64) uint64 {
	c := uint64(1)
	for c < uint64(max(n, 1)) {
		c <<= 1
	}
	return c
}

func recBytes(msg Record) int64 { return int64(len(msg.Payload)) + spec.RecordOverhead }

func (r *ringCache) Put(msg Record) {
	size := recBytes(msg)
	r.mu.Lock()
	if !r.started {
		r.started = true
		r.head = msg.Offset
	}
	r.slots[msg.Offset&r.mask] = msg
	r.curBytes += size
	for r.curBytes > r.maxBytes && r.head <= msg.Offset {
		slot := &r.slots[r.head&r.mask]
		if !slot.Timestamp.IsZero() && slot.Offset == r.head {
			r.curBytes -= recBytes(*slot)
			*slot = Record{}
		}
		r.head++
	}
	r.mu.Unlock()
}

func (r *ringCache) Get(offset uint64) (Record, bool) {
	r.mu.Lock()
	msg := r.slots[offset&r.mask]
	r.mu.Unlock()
	if msg.Offset == offset && !msg.Timestamp.IsZero() {
		return msg, true
	}
	return Record{}, false
}

// SetMaxBytes moves the byte budget and evicts from the head until the cache fits.
func (r *ringCache) SetMaxBytes(maxBytes int64) {
	r.mu.Lock()
	r.maxBytes = maxBytes
	for r.curBytes > r.maxBytes {
		slot := &r.slots[r.head&r.mask]
		if !slot.Timestamp.IsZero() && slot.Offset == r.head {
			r.curBytes -= recBytes(*slot)
			*slot = Record{}
		}
		if r.head++; r.head == 0 {
			break
		}
	}
	r.mu.Unlock()
}

func (r *ringCache) EvictBefore(minOffset uint64) {
	r.mu.Lock()
	for i := range r.slots {
		if !r.slots[i].Timestamp.IsZero() && r.slots[i].Offset < minOffset {
			r.curBytes -= recBytes(r.slots[i])
			r.slots[i] = Record{}
		}
	}
	r.mu.Unlock()
}

// Evict removes the specific offsets dropped out of order by compaction.
func (r *ringCache) Evict(offsets ...uint64) {
	if len(offsets) == 0 {
		return
	}
	r.mu.Lock()
	for _, off := range offsets {
		slot := &r.slots[off&r.mask]
		if slot.Offset == off && !slot.Timestamp.IsZero() {
			r.curBytes -= recBytes(*slot)
			*slot = Record{}
		}
	}
	r.mu.Unlock()
}
