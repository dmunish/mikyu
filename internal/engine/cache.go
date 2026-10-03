package engine

import (
	"sync"
	"sync/atomic"

	"github.com/dmunish/mikyu/internal/spec"
)

const (
	minRingCacheSlots    = 64
	maxRingCacheSlots    = 65536
	ringCacheAvgRecBytes = 256
)

// cacheSlot holds one ring entry; the record is published through an atomic
// pointer so readers never take the cache mutex and never observe torn records.
type cacheSlot struct {
	rec atomic.Pointer[Record]
}

type ringCache struct {
	mu       sync.Mutex
	slots    []cacheSlot
	mask     uint64
	maxBytes int64
	curBytes int64
	head     uint64 // next offset to evict; advances FIFO as the byte budget is exceeded
	started  bool
}

func newRingCache(maxBytes int64) *ringCache {
	capacity := min(max(nextPow2(maxBytes/ringCacheAvgRecBytes), minRingCacheSlots), maxRingCacheSlots)
	return &ringCache{
		slots:    make([]cacheSlot, capacity),
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
	rec := msg
	r.slots[msg.Offset&r.mask].rec.Store(&rec)
	r.curBytes += size
	for r.curBytes > r.maxBytes && r.head <= msg.Offset {
		r.clearSlot(&r.slots[r.head&r.mask], r.head)
		r.head++
	}
	r.mu.Unlock()
}

func (r *ringCache) Get(offset uint64) (Record, bool) {
	rec := r.slots[offset&r.mask].rec.Load()
	if rec != nil && rec.Offset == offset && !rec.Timestamp.IsZero() {
		return *rec, true
	}
	return Record{}, false
}

// clearSlot drops one slot's record so concurrent readers observe a miss.
func (r *ringCache) clearSlot(slot *cacheSlot, offset uint64) {
	if rec := slot.rec.Load(); rec != nil && rec.Offset == offset && !rec.Timestamp.IsZero() {
		r.curBytes -= recBytes(*rec)
		slot.rec.Store(nil)
	}
}

// stats returns the cache's current and maximum byte budgets.
func (r *ringCache) stats() (cur, max int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.curBytes, r.maxBytes
}

// SetMaxBytes moves the byte budget and evicts from the head until the cache fits.
func (r *ringCache) SetMaxBytes(maxBytes int64) {
	r.mu.Lock()
	r.maxBytes = maxBytes
	for r.curBytes > r.maxBytes {
		r.clearSlot(&r.slots[r.head&r.mask], r.head)
		if r.head++; r.head == 0 {
			break
		}
	}
	r.mu.Unlock()
}

func (r *ringCache) EvictBefore(minOffset uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.slots {
		slot := &r.slots[i]
		if rec := slot.rec.Load(); rec != nil && !rec.Timestamp.IsZero() && rec.Offset < minOffset {
			r.curBytes -= recBytes(*rec)
			slot.rec.Store(nil)
		}
	}
}

// Evict removes the specific offsets dropped out of order by compaction.
func (r *ringCache) Evict(offsets ...uint64) {
	if len(offsets) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, off := range offsets {
		r.clearSlot(&r.slots[off&r.mask], off)
	}
}
