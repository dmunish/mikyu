package engine

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// writerHook, when non-nil, runs before every WAL file write so tests can assert the single-writer invariant.
var writerHook func()

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
//
// Single writer: exactly one goroutine (the syncLoop) writes bytes to the WAL
// segment file. Producers only stage records via push/pushBatch (sync path) or
// pushAsync (async path); flush and drainAndFlushAsync are the sole callers of
// writeAt. Async backpressure is bounded: asyncChan holds BatchSize entries,
// a full channel blocks the producer, and a canceled context yields ctx.Err().
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

	asyncChan     chan asyncEntry // Lock-free channel for fsync: false fast-path
	wake          chan struct{}
	configChanged chan struct{}
	shutdown      chan struct{}
	done          chan struct{}
	stopOnce      sync.Once

	m *walMetrics
}

func newWAL(f *os.File, cfg Config, startOffset int64, scopes ...*metrics.Scope) *wal {
	cfg = cfg.withDefaults()
	initCap := cfg.BatchSize * 1024
	w := &wal{
		file:          f,
		cfg:           cfg,
		published:     startOffset,
		pushed:        startOffset,
		buf:           make([]byte, 0, initCap),
		pending:       make([]pendingEntry, 0, cfg.BatchSize),
		swapBuf:       make([]byte, 0, initCap),
		swapPending:   make([]pendingEntry, 0, cfg.BatchSize),
		asyncChan:     make(chan asyncEntry, cfg.BatchSize),
		wake:          make(chan struct{}, 1),
		configChanged: make(chan struct{}, 1),
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
		m:             newWALMetrics(scopeOf(scopes)),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *wal) start() {
	go w.syncLoop()
}

func (w *wal) syncLoop() {
	defer close(w.done)

	w.mu.Lock()
	interval := w.cfg.MaxInterval
	w.mu.Unlock()

	ticker := time.NewTicker(interval)

	for {
		select {
		case entry := <-w.asyncChan:
			w.drainAndFlushAsync(entry)
		case <-ticker.C:
			w.flush()
		case <-w.wake:
			w.adaptiveFlush()
		case <-w.configChanged:
			ticker.Stop()
			w.mu.Lock()
			interval = w.cfg.MaxInterval
			w.mu.Unlock()
			ticker = time.NewTicker(interval)
		case <-w.shutdown:
			ticker.Stop()
			w.shutdownFlush()
			return
		}
	}
}

// UpdateConfig swaps the WAL's batching and timing parameters.
func (w *wal) UpdateConfig(batchSize int, maxInterval, minInterval time.Duration) {
	w.mu.Lock()
	w.cfg.BatchSize = batchSize
	w.cfg.MaxInterval = maxInterval
	w.cfg.MinInterval = minInterval
	w.mu.Unlock()
	select {
	case w.configChanged <- struct{}{}:
	default:
	}
}

func (w *wal) shutdownFlush() {
	w.drainRemainingAsync()
	w.flush()
	if err := syncData(w.file); err != nil {
		w.setError(err)
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
	w.m.pending.Set(int64(len(w.pending)))

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
	w.m.pending.Set(int64(len(w.pending)))

	wake = len(w.pending) >= w.cfg.BatchSize || len(recs) >= w.cfg.BatchSize
	return seq, wake, nil
}

// writeAt is the only writer of the WAL segment file; flush and drainAndFlushAsync call it.
func (w *wal) writeAt(p []byte, off int64) (int, error) {
	if writerHook != nil {
		writerHook()
	}
	return w.file.WriteAt(p, off)
}

// pushAsync stages one record on the async path, blocking while the channel is full and honoring ctx.
func (w *wal) pushAsync(ctx context.Context, e asyncEntry) error {
	select {
	case w.asyncChan <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return ErrQueueClosed
	}
}

func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.published <= seq && w.err == nil {
		w.cond.Wait()
	}
	return w.err
}

func (w *wal) currentError() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *wal) adaptiveFlush() {
	w.mu.Lock()
	n := len(w.pending)
	batchSize := w.cfg.BatchSize
	minInterval := w.cfg.MinInterval
	w.mu.Unlock()

	if n == 0 {
		return
	}

	if n < batchSize && minInterval > 0 {
		time.Sleep(minInterval)
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
	fsync := w.cfg.Fsync

	w.buf = w.swapBuf[:0]
	w.pending = w.swapPending[:0]

	// Drain wake signal associated ONLY with the buffers being flushed
	select {
	case <-w.wake:
	default:
	}

	w.mu.Unlock()

	start := time.Now()
	var writeErr error
	if _, err := w.writeAt(data, entries[0].pos); err != nil {
		writeErr = err
	} else if fsync {
		if err := syncData(w.file); err != nil {
			writeErr = err
		}
	}
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())

	w.mu.Lock()
	if writeErr != nil {
		w.err = writeErr
	} else {
		w.published += int64(len(entries))
	}

	// Reclaim used buffer capacity for next swap cycle
	w.swapBuf = data[:0]
	w.swapPending = entries[:0]
	w.m.pending.Set(int64(len(w.pending)))

	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *wal) drainAndFlushAsync(first asyncEntry) {
	w.mu.Lock()
	batchSize := w.cfg.BatchSize
	w.mu.Unlock()

	entries := []asyncEntry{first}

	for len(entries) < batchSize {
		select {
		case entry := <-w.asyncChan:
			entries = append(entries, entry)
		default:
			goto WRITE
		}
	}

WRITE:
	w.mu.Lock()
	failed := w.err
	published := uint64(w.published)
	w.mu.Unlock()
	if failed != nil {
		return
	}

	start := time.Now()
	for _, e := range entries {
		if _, err := w.writeAt(e.rec, e.pos); err != nil {
			w.setError(err)
			return
		}
		idxPos := int64(e.offset-e.seg.baseOff) * rawIndexEntrySize
		if err := e.seg.writeIndexAt(idxPos, e.pos, e.recLen); err != nil {
			w.setError(err)
			return
		}
		if e.offset == published {
			published++
		}
	}
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())

	w.mu.Lock()
	if published > uint64(w.published) {
		w.published = int64(published)
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *wal) setError(err error) {
	if err == nil {
		return
	}
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.cond.Broadcast()
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
	w.stopOnce.Do(func() {
		close(w.shutdown)
		<-w.done
	})
	return w.err
}
