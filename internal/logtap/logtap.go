// Package logtap fans each rendered log line out to a bounded in-memory ring and any number of live followers.
package logtap

import (
	"bytes"
	"sync"
)

// DefaultCapacity is the ring size a daemon keeps for log replay.
const DefaultCapacity = 4096

// followerBuffer bounds how many lines a follower may fall behind before it
// is dropped; a slow follower never backpressures the daemon.
const followerBuffer = 256

// Tap is the daemon-side sink of the log stream: Write appends one rendered line to the ring and delivers it to every live follower, dropping a follower that cannot keep up rather than backpressuring the daemon.
type Tap struct {
	mu        sync.Mutex
	ring      [][]byte
	head      int
	count     int
	pending   []byte
	followers map[*subscription]struct{}
}

// subscription is one live follower of the tap.
type subscription struct {
	ch     chan []byte
	closed bool
}

// New returns a tap holding the last capacity lines.
func New(capacity int) *Tap {
	if capacity < 1 {
		capacity = DefaultCapacity
	}
	return &Tap{
		ring:      make([][]byte, capacity),
		followers: make(map[*subscription]struct{}),
	}
}

// Write implements io.Writer for a rendered log line; bytes accumulate until a newline completes a line, so one record never fans out as fragments.
func (t *Tap) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pending = append(t.pending, p...)
	for {
		i := bytes.IndexByte(t.pending, '\n')
		if i < 0 {
			break
		}
		line := append([]byte(nil), t.pending[:i]...)
		rest := append([]byte(nil), t.pending[i+1:]...)
		t.pending = rest
		t.append(line)
	}
	return len(p), nil
}

// append stores one complete line and fans it out; caller holds t.mu.
func (t *Tap) append(line []byte) {
	if len(line) == 0 {
		return
	}
	i := (t.head + t.count) % len(t.ring)
	if t.count == len(t.ring) {
		t.ring[i] = line
		t.head = (t.head + 1) % len(t.ring)
	} else {
		t.ring[i] = line
		t.count++
	}
	for f := range t.followers {
		select {
		case f.ch <- line:
		default:
			// The follower cannot keep up: drop it so a stalled consumer
			// never backpressures the daemon. Closing the channel ends its
			// stream; the client reattaches and replays the ring.
			f.closed = true
			close(f.ch)
			delete(t.followers, f)
		}
	}
}

// Snapshot returns a copy of the ring, oldest line first.
func (t *Tap) Snapshot() [][]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([][]byte, 0, t.count)
	for i := range t.count {
		line := t.ring[(t.head+i)%len(t.ring)]
		out = append(out, append([]byte(nil), line...))
	}
	return out
}

// Subscribe registers a live follower under the ring lock and returns the backlog up to this instant, so replay and tail never overlap or gap; the channel closes when the follower is dropped for falling behind.
func (t *Tap) Subscribe() (backlog [][]byte, ch <-chan []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	sub := &subscription{ch: make(chan []byte, followerBuffer)}
	t.followers[sub] = struct{}{}
	return t.snapshotLocked(), sub.ch
}

// Unsubscribe removes a follower; a missing or already-dropped one is a no-op.
func (t *Tap) Unsubscribe(ch <-chan []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for f := range t.followers {
		if f.ch == ch {
			delete(t.followers, f)
			return
		}
	}
}

// snapshotLocked copies the ring; caller holds t.mu.
func (t *Tap) snapshotLocked() [][]byte {
	out := make([][]byte, 0, t.count)
	for i := range t.count {
		line := t.ring[(t.head+i)%len(t.ring)]
		out = append(out, append([]byte(nil), line...))
	}
	return out
}
