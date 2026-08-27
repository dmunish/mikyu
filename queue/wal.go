package queue

import (
	"os"
	"sync"
	"time"
)

type Config struct {
	BatchSize int
	Interval  time.Duration
}

func (c Config) withDefaults() Config {
	if c.BatchSize <= 0 {
		c.BatchSize = 256
	}
	if c.Interval <= 0 {
		c.Interval = 100 * time.Millisecond
	}
	return c
}

type pendingEntry struct {
	pos int64
	len int64
}

type wal struct {
	cfg  Config
	file *os.File

	mu        sync.Mutex
	cond      *sync.Cond
	buf       []byte         // Staged bytes
	pending   []pendingEntry // Staged index metadata
	published int64          // Count of flushed records on disk
	err       error          // Sticky error state

	wake     chan struct{}
	shutdown chan struct{}
	done     chan struct{}
}

func newWAL(f *os.File, cfg Config) *wal {
	fl := &wal{
		file:     f,
		cfg:      cfg.withDefaults(),
		wake:     make(chan struct{}, 1),
		shutdown: make(chan struct{}),
		done:     make(chan struct{}),
	}
	fl.cond = sync.NewCond(&fl.mu)
	return fl
}

func (w *wal) start() {
	go w.syncLoop()
}

func (w *wal) syncLoop() {
	defer close(w.done)
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-w.wake:
		case <-w.shutdown:
			w.flush()
			return
		}
		w.flush()
	}
}

// push stages a record and returns its assignable sequence number
func (w *wal) push(rec []byte, pos int64) (seq int64, full bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	seq = w.published + int64(len(w.pending))
	w.buf = append(w.buf, rec...)
	w.pending = append(w.pending, pendingEntry{pos: pos, len: int64(len(rec))})
	full = len(w.pending) >= w.cfg.BatchSize

	return seq, full, nil
}

// wait blocks until seq is durable on disk or a fatal flush error occurs
func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.published <= seq && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *wal) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 || w.err != nil {
		w.mu.Unlock()
		return
	}

	// Swap out buffers to keep locks fast
	data := w.buf
	entries := w.pending
	w.buf = nil
	w.pending = nil
	w.mu.Unlock()

	// Perform disk I/O without holding the mutex
	var writeErr error
	if _, err := w.file.WriteAt(data, entries[0].pos); err != nil {
		writeErr = err
	} else if err := w.file.Sync(); err != nil {
		writeErr = err
	}

	w.mu.Lock()
	if writeErr != nil {
		w.err = writeErr
	} else {
		w.published += int64(len(entries))
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *wal) stop() error {
	close(w.shutdown)
	<-w.done
	return w.err
}
