package engine

import (
	"os"
	"sync"
	"time"
)

type pendingEntry struct {
	pos int64
	len int64
}

// wal manages write-ahead logging using dynamic dual-threshold flusher batching
// (flush when pending count >= BatchSize OR time interval elapsed).
type wal struct {
	cfg  Config
	file *os.File

	mu        sync.Mutex
	cond      *sync.Cond
	buf       []byte
	pending   []pendingEntry
	pushed    int64
	published int64
	err       error

	wake     chan struct{}
	shutdown chan struct{}
	done     chan struct{}
}

func newWAL(f *os.File, cfg Config, startOffset int64) *wal {
	initCap := cfg.BatchSize * 1024
	w := &wal{
		file:      f,
		cfg:       cfg.withDefaults(),
		published: startOffset,
		pushed:    startOffset,
		buf:       make([]byte, 0, initCap),
		pending:   make([]pendingEntry, 0, cfg.BatchSize),
		wake:      make(chan struct{}, 1),
		shutdown:  make(chan struct{}),
		done:      make(chan struct{}),
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

func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	seq = w.pushed
	w.pushed++
	w.buf = append(w.buf, rec...)
	w.pending = append(w.pending, pendingEntry{pos: pos, len: int64(len(rec))})

	wake = len(w.pending) == 1 || len(w.pending) >= w.cfg.BatchSize
	return seq, wake, nil
}

func (w *wal) pushBatch(recs [][]byte, positions []int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	seq = w.pushed + int64(len(recs)) - 1
	w.pushed += int64(len(recs))

	for i, rec := range recs {
		w.buf = append(w.buf, rec...)
		w.pending = append(w.pending, pendingEntry{pos: positions[i], len: int64(len(rec))})
	}

	wake = len(w.pending) >= w.cfg.BatchSize || len(recs) >= w.cfg.BatchSize
	return seq, wake, nil
}

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

	// Swap staged buffers to keep mutex lock duration minimal during disk I/O
	data := w.buf
	entries := w.pending
	w.buf = nil
	w.pending = nil
	w.mu.Unlock()

	var writeErr error
	if _, err := w.file.WriteAt(data, entries[0].pos); err != nil {
		writeErr = err
	} else if err := syncData(w.file); err != nil {
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
