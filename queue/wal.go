package queue

import (
	"os"
	"sync"
	"time"
)

type pendingEntry struct {
	pos int64 //per-segment pointer
	len int64
}

type wal struct {
	cfg  Config
	file *os.File

	mu   sync.Mutex
	cond *sync.Cond

	buf          []byte         // Active staging buffer
	flushBuf     []byte         // Swap buffer used during disk write
	pending      []pendingEntry // Active index entries
	flushPending []pendingEntry // Swap index entries

	published int64 // Count of durable records on disk
	err       error // Sticky error state

	wake     chan struct{}
	shutdown chan struct{}
	done     chan struct{}
}

func newWAL(f *os.File, cfg Config, startOffset int64) *wal {
	initCap := cfg.BatchSize * 1024 // ~256 KiB
	w := &wal{
		file:         f,
		cfg:          cfg.withDefaults(),
		published:    startOffset,
		buf:          make([]byte, 0, initCap),
		flushBuf:     make([]byte, 0, initCap),
		pending:      make([]pendingEntry, 0, cfg.BatchSize),
		flushPending: make([]pendingEntry, 0, cfg.BatchSize),
		wake:         make(chan struct{}, 1),
		shutdown:     make(chan struct{}),
		done:         make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
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

// push stages a record and signals if the flusher should wake immediately.
func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	seq = w.published + int64(len(w.pending))
	w.buf = append(w.buf, rec...)
	w.pending = append(w.pending, pendingEntry{pos: pos, len: int64(len(rec))})

	wake = len(w.pending) == 1 || len(w.pending) >= w.cfg.BatchSize

	return seq, wake, nil
}

// wait blocks until seq is durable on disk or a fatal error occurs.
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

	// Swap active buffers with flush buffers (reserving capacity, resetting length to 0)
	w.buf, w.flushBuf = w.flushBuf[:0], w.buf
	w.pending, w.flushPending = w.flushPending[:0], w.pending

	data := w.flushBuf
	entries := w.flushPending
	w.mu.Unlock()

	// Perform disk I/O without holding the lock
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
