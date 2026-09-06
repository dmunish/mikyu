package engine

import "sync"

const defaultRingCacheCapacity = 16384

type ringCache struct {
	mu    sync.RWMutex
	slots []Message
	mask  uint64
}

func newRingCache(capacity uint64) *ringCache {
	return &ringCache{
		slots: make([]Message, capacity),
		mask:  capacity - 1,
	}
}

func (r *ringCache) Put(msg Message) {
	r.mu.Lock()
	r.slots[msg.Offset&r.mask] = msg
	r.mu.Unlock()
}

func (r *ringCache) Get(offset uint64) (Message, bool) {
	r.mu.RLock()
	msg := r.slots[offset&r.mask]
	r.mu.RUnlock()
	if msg.Offset == offset && !msg.Timestamp.IsZero() {
		return msg, true
	}
	return Message{}, false
}
