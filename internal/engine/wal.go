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

type asyncEntry struct {
	seg    *segment
	rec    []byte
	pos    int64
	recLen int64
	offset uint64
}

// wal manages write-ahead logging using adaptive dual-threshold flusher batching
// with micro-linger group commits to minimize fsync overhead.
type wal struct {
	cfg  Config
	file *os.File

	mu          sync.Mutex
	cond        *sync.Cond
	buf         []byte
	pending     []pendingEntry
	swapBuf     []byte
	swapPending []pendingEntry
	pushed      int64
	published   int64
	err         error

	asyncChan chan asyncEntry // Lock-free channel for fsync: false fast-path
	wake      chan struct{}
	shutdown  chan struct{}
	done      chan struct{}
}

func newWAL(f *os.File, cfg Config, startOffset int64) *wal {
	initCap := cfg.BatchSize * 1024
	w := &wal{
		file:        f,
		cfg:         cfg.withDefaults(),
		published:   startOffset,
		pushed:      startOffset,
		buf:         make([]byte, 0, initCap),
		pending:     make([]pendingEntry, 0, cfg.BatchSize),
		swapBuf:     make([]byte, 0, initCap),
		swapPending: make([]pendingEntry, 0, cfg.BatchSize),
		asyncChan:   make(chan asyncEntry, 65536),
		wake:        make(chan struct{}, 1),
		shutdown:    make(chan struct{}),
		done:        make(chan struct{}),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *wal) start() {
	go w.syncLoop()
}

func (w *wal) syncLoop() {
	defer close(w.done)
	ticker := time.NewTicker(w.cfg.MaxInterval)
	defer ticker.Stop()

	for {
		select {
		case entry := <-w.asyncChan:
			w.drainAndFlushAsync(entry)
		case <-ticker.C:
			w.flush()
		case <-w.wake:
			w.adaptiveFlush()
		case <-w.shutdown:
			w.drainRemainingAsync()
			w.flush()
			_ = syncData(w.file) // Always force physical disk sync on shutdown
			return
		}
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

func (w *wal) adaptiveFlush() {
	w.mu.Lock()
	n := len(w.pending)
	w.mu.Unlock()

	if n == 0 {
		return
	}

	if n < w.cfg.BatchSize && w.cfg.MinInterval > 0 {
		time.Sleep(w.cfg.MinInterval)
	}

	w.flush()
}

func (w *wal) flush() {
	w.mu.Lock()
	if len(w.buf) == 0 || w.err != nil {
		w.mu.Unlock()
		return
	}

	// Swap staged buffers with pre-allocated swap buffers
	data := w.buf
	entries := w.pending

	w.buf = w.swapBuf[:0]
	w.pending = w.swapPending[:0]

	// Drain wake signal associated ONLY with the buffers being flushed
	select {
	case <-w.wake:
	default:
	}

	w.mu.Unlock()

	var writeErr error
	if _, err := w.file.WriteAt(data, entries[0].pos); err != nil {
		writeErr = err
	} else if w.cfg.Fsync {
		// Only execute physical disk sync if Fsync is enabled
		if err := syncData(w.file); err != nil {
			writeErr = err
		}
	}

	w.mu.Lock()
	if writeErr != nil {
		w.err = writeErr
	} else {
		w.published += int64(len(entries))
	}

	// Reclaim used buffer capacity for next swap cycle
	w.swapBuf = data[:0]
	w.swapPending = entries[:0]

	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *wal) drainAndFlushAsync(first asyncEntry) {
	entries := []asyncEntry{first}

	// Drain up to BatchSize entries from the channel without blocking
	for len(entries) < w.cfg.BatchSize {
		select {
		case entry := <-w.asyncChan:
			entries = append(entries, entry)
		default:
			goto WRITE
		}
	}

WRITE:
	var maxOff uint64
	for _, e := range entries {
		if _, err := e.seg.logFile.WriteAt(e.rec, e.pos); err == nil {
			idxPos := int64(e.offset-e.seg.baseOff) * 16
			_ = e.seg.writeIndexAt(idxPos, e.pos, e.recLen)
		}
		if e.offset > maxOff {
			maxOff = e.offset
		}
	}

	w.mu.Lock()
	if int64(maxOff+1) > w.published {
		w.published = int64(maxOff + 1)
	}
	w.mu.Unlock()
}

func (w *wal) drainRemainingAsync() {
	for {
		select {
		case entry := <-w.asyncChan:
			w.drainAndFlushAsync(entry)
		default:
			return
		}
	}
}

func (w *wal) stop() error {
	close(w.shutdown)
	<-w.done
	return w.err
}
