package metrics

import "sync"

// RingBuffer is a fixed-capacity, concurrency-safe FIFO buffer.
type RingBuffer[T any] struct {
	mu   sync.Mutex
	buf  []T
	head int
	size int
}

// NewRingBuffer allocates a ring buffer holding at most capacity values.
func NewRingBuffer[T any](capacity int) *RingBuffer[T] {
	if capacity < 1 {
		capacity = 1
	}
	return &RingBuffer[T]{buf: make([]T, capacity)}
}

// Push appends v, evicting the oldest value when full.
func (r *RingBuffer[T]) Push(v T) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size < len(r.buf) {
		r.buf[(r.head+r.size)%len(r.buf)] = v
		r.size++
		return
	}
	r.buf[r.head] = v
	r.head = (r.head + 1) % len(r.buf)
}

// Snapshot returns a copy of the buffered values, oldest first.
func (r *RingBuffer[T]) Snapshot() []T {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]T, r.size)
	for i := range r.size {
		out[i] = r.buf[(r.head+i)%len(r.buf)]
	}
	return out
}

// Latest returns the most recently pushed value.
func (r *RingBuffer[T]) Latest() (T, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.size == 0 {
		var zero T
		return zero, false
	}
	return r.buf[(r.head+r.size-1)%len(r.buf)], true
}

// Len reports how many values are buffered.
func (r *RingBuffer[T]) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.size
}
