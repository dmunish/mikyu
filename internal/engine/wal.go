package engine

import (
	"context"
	"os"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
)

// writerHook, when non-nil, runs before every WAL file write so tests can assert the single-writer invariant.
var writerHook func()

// writeAtHook, when non-nil, receives every WAL log write's offset and length so tests can assert batching.
var writeAtHook func(off int64, n int)

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

// wal manages write-ahead logging using byte-budgeted flusher batching with
// micro-linger group commits to minimize fsync overhead.
//
// Single writer: exactly one goroutine (the syncLoop) writes bytes to the WAL
// segment file. Producers only stage records via push/pushBatch (sync path) or
// pushAsync (async path); flush and drainAndFlushAsync are the sole callers of
// writeAt. Sequencing is byte-based: pushedBytes/publishedBytes count staged
// and flushed bytes, and wait(seq) blocks until publishedBytes reaches the
// byte offset the push returned. Async backpressure is bounded: asyncChan
// holds MaxFlushBytes/4096 slots (a byte-to-slot approximation), a full
// channel blocks the producer, and a canceled context yields ctx.Err().
type wal struct {
	cfg  Config
	file *os.File

	mu             sync.Mutex
	cond           *sync.Cond
	buf            []byte
	pending        []pendingEntry
	pendingBytes   int64
	swapBuf        []byte
	swapPending    []pendingEntry
	pushedBytes    int64
	publishedBytes int64
	asyncNext      uint64
	err            error

	asyncChan     chan asyncEntry // Lock-free channel for the async fast path
	wake          chan struct{}
	configChanged chan struct{}
	shutdown      chan struct{}
	done          chan struct{}
	stopOnce      sync.Once

	m *walMetrics
}

func newWAL(f *os.File, cfg Config, startOffset uint64, scopes ...*metrics.Scope) *wal {
	flushBytes := max(cfg.MaxFlushBytes, 4096)
	w := &wal{
		file:          f,
		cfg:           cfg,
		asyncNext:     startOffset,
		buf:           make([]byte, 0, flushBytes),
		pending:       make([]pendingEntry, 0, 64),
		swapBuf:       make([]byte, 0, flushBytes),
		swapPending:   make([]pendingEntry, 0, 64),
		asyncChan:     make(chan asyncEntry, max(1, int(cfg.MaxFlushBytes)/4096)),
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
	interval := w.cfg.MaxFlushWait
	w.mu.Unlock()
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}

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
			interval = w.cfg.MaxFlushWait
			w.mu.Unlock()
			if interval <= 0 {
				interval = 10 * time.Millisecond
			}
			ticker = time.NewTicker(interval)
		case <-w.shutdown:
			ticker.Stop()
			w.shutdownFlush()
			return
		}
	}
}

// UpdateConfig swaps the WAL's byte budget and timing parameters.
func (w *wal) UpdateConfig(maxFlushBytes int64, maxFlushWait, flushLinger time.Duration) {
	w.mu.Lock()
	w.cfg.MaxFlushBytes = maxFlushBytes
	w.cfg.MaxFlushWait = maxFlushWait
	w.cfg.FlushLinger = flushLinger
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

	n := int64(len(rec))
	seq = w.pushedBytes + n
	w.pushedBytes = seq
	w.buf = append(w.buf, rec...)
	w.pending = append(w.pending, pendingEntry{pos: pos, len: n})
	w.pendingBytes += n
	w.m.pending.Set(int64(len(w.pending)))

	wake = w.pendingBytes >= w.cfg.MaxFlushBytes
	return seq, wake, nil
}

func (w *wal) pushBatch(recs [][]byte, positions []int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	var total int64
	for i, rec := range recs {
		w.buf = append(w.buf, rec...)
		w.pending = append(w.pending, pendingEntry{pos: positions[i], len: int64(len(rec))})
		total += int64(len(rec))
	}
	w.pendingBytes += total
	w.pushedBytes += total
	seq = w.pushedBytes
	w.m.pending.Set(int64(len(w.pending)))

	wake = w.pendingBytes >= w.cfg.MaxFlushBytes
	return seq, wake, nil
}

// writeAt is the only writer of the WAL segment file; flush and drainAndFlushAsync call it.
func (w *wal) writeAt(p []byte, off int64) (int, error) {
	if writerHook != nil {
		writerHook()
	}
	n, err := w.file.WriteAt(p, off)
	if writeAtHook != nil {
		writeAtHook(off, n)
	}
	return n, err
}

// pushAsync stages one record on the async path, blocking while the channel is full and honoring ctx.
func (w *wal) pushAsync(ctx context.Context, e asyncEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case w.asyncChan <- e:
		return nil
	default:
	}
	w.m.blockEvents.Inc()
	w.m.blockedProducers.Add(1)
	defer w.m.blockedProducers.Add(-1)
	select {
	case w.asyncChan <- e:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return ErrQueueClosed
	}
}

// bytes returns the WAL's committed, flushed, and staged byte counts.
func (w *wal) bytes() (pushed, published, pending int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.pushedBytes, w.publishedBytes, w.pendingBytes
}

// channelFill reports the async channel occupancy as a fraction in [0,1].
func (w *wal) channelFill() float64 {
	if capacity := cap(w.asyncChan); capacity > 0 {
		return min(float64(len(w.asyncChan))/float64(capacity), 1)
	}
	return 0
}

// byteFill reports staged WAL bytes as a fraction of the flush budget.
func (w *wal) byteFill() float64 {
	w.mu.Lock()
	pending, maxBytes := w.pendingBytes, w.cfg.MaxFlushBytes
	w.mu.Unlock()
	if maxBytes <= 0 {
		return 0
	}
	return min(float64(pending)/float64(maxBytes), 1)
}

// wait blocks until the flusher has published every byte staged before seq.
func (w *wal) wait(seq int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for w.publishedBytes < seq && w.err == nil {
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
	pending := w.pendingBytes
	maxBytes := w.cfg.MaxFlushBytes
	linger := w.cfg.FlushLinger
	w.mu.Unlock()

	if pending == 0 {
		return
	}

	if pending < maxBytes && linger > 0 {
		time.Sleep(linger)
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
	w.pendingBytes = 0

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
	} else if w.cfg.Durability != spec.DurabilityAsync {
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
		w.publishedBytes += int64(len(data))
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
	maxBytes := w.cfg.MaxFlushBytes
	w.mu.Unlock()

	entries := []asyncEntry{first}
	total := first.recLen

	for total < maxBytes {
		select {
		case entry := <-w.asyncChan:
			entries = append(entries, entry)
			total += entry.recLen
		default:
			goto WRITE
		}
	}

WRITE:
	w.mu.Lock()
	failed := w.err
	next := w.asyncNext
	w.mu.Unlock()
	if failed != nil {
		return
	}

	start := time.Now()
	for i := 0; i < len(entries); {
		j := i + 1
		for j < len(entries) && entries[j].seg == entries[i].seg &&
			entries[j].pos == entries[j-1].pos+entries[j-1].recLen {
			j++
		}
		if err := w.writeRun(entries[i:j], &next); err != nil {
			w.setError(err)
			return
		}
		i = j
	}
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())

	w.mu.Lock()
	if next > w.asyncNext {
		w.asyncNext = next
	}
	w.cond.Broadcast()
	w.mu.Unlock()
}

// writeRun coalesces one contiguous same-segment run into a single log write and writes its index entries.
func (w *wal) writeRun(run []asyncEntry, next *uint64) error {
	data := run[0].rec
	if len(run) > 1 {
		var totalLen int64
		for _, e := range run {
			totalLen += e.recLen
		}
		buf := make([]byte, 0, totalLen)
		for _, e := range run {
			buf = append(buf, e.rec...)
		}
		data = buf
	}
	if _, err := w.writeAt(data, run[0].pos); err != nil {
		return err
	}
	positions := make([]int64, len(run))
	lengths := make([]int64, len(run))
	for i, e := range run {
		positions[i], lengths[i] = e.pos, e.recLen
		if e.offset == *next {
			*next++
		}
	}
	idxPos := int64(run[0].offset-run[0].seg.baseOff) * rawIndexEntrySize
	return run[0].seg.writeIndexBatchAt(idxPos, positions, lengths)
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
