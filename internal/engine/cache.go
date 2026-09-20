package engine

import "sync"

const defaultRingCacheCapacity = 16384

type ringCache struct {
	mu    sync.RWMutex
	slots []Record
	mask  uint64
}

func newRingCache(capacity uint64) *ringCache {
	return &ringCache{
		slots: make([]Record, capacity),
		mask:  capacity - 1,
	}
}

func (r *ringCache) Put(msg Record) {
	r.mu.Lock()
	r.slots[msg.Offset&r.mask] = msg
	r.mu.Unlock()
}

func (r *ringCache) Get(offset uint64) (Record, bool) {
	r.mu.RLock()
	msg := r.slots[offset&r.mask]
	r.mu.RUnlock()
	if msg.Offset == offset && !msg.Timestamp.IsZero() {
		return msg, true
	}
	return Record{}, false
}

func (r *ringCache) EvictBefore(minOffset uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := range r.slots {
		if !r.slots[i].Timestamp.IsZero() && r.slots[i].Offset < minOffset {
			r.slots[i] = Record{}
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
		if r.slots[off&r.mask].Offset == off {
			r.slots[off&r.mask] = Record{}
		}
	}
}
