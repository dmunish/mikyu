package engine

import (
	"context"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/iow"
	"github.com/dmunish/mikyu/internal/metrics"
)

// newWriter builds the WAL's iow.Writer; tests replace it to hold batches.
var newWriter = iow.New

// maxAsyncOutstanding bounds the async path's submitted-but-unwritten batches.
func maxAsyncOutstanding(cfg Config) int {
	return max(1, int(cfg.MaxFlushBytes)/4096)
}

// iowDurability maps the queue's durability onto the writer's completion boundary.
func iowDurability(cfg Config) iow.Durability {
	if cfg.Async() {
		return iow.Buffered
	}
	return iow.DataSync
}

// staged holds the sync path's unflushed bytes and the entries that place them in the segment.
type staged struct {
	data    []byte
	pending []pendingEntry
}

// newSyscallWAL builds the pool-backed WAL: sync batches stage in byte-budgeted
// buffers behind one flusher goroutine with micro-linger group commits, and
// async batches submit straight to the writer's bounded worker pool.
//
// The segment owns its file handles, so the writer is built without them: the
// WAL fsyncs the log through SyncFiles and never closes what it did not open.
func newSyscallWAL(seg *segment, cfg Config, startOffset uint64, scopes ...*metrics.Scope) *wal {
	durability := iowDurability(cfg)
	workers := runtime.GOMAXPROCS(0)
	if durability == iow.DataSync {
		// One durable batch at a time: the kernel serializes fsyncs per file
		// anyway, and concurrent ones each pay for the other's dirty pages.
		workers = 1
	}
	writer, err := newWriter(nil, iow.Options{
		Workers: workers,
		// The slot budget is the async backpressure bound, so the writer's
		// queue is at least as deep: producers block on a measurable slot, not
		// inside the pool, which is where the dirty-page throttle would stall
		// them invisibly.
		Outstanding: maxAsyncOutstanding(cfg),
		Durability:  durability,
		SyncFiles:   []*os.File{seg.logFile},
	})
	w := &wal{
		cfg:           cfg,
		files:         &sink{log: seg.logFile, idx: seg.idxFile},
		writer:        writer,
		asyncNext:     startOffset,
		stage:         newStaged(cfg.MaxFlushBytes),
		slots:         make(chan struct{}, maxAsyncOutstanding(cfg)),
		wake:          make(chan struct{}, 1),
		configChanged: make(chan struct{}, 1),
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
		m:             newWALMetrics(scopeOf(scopes)),
	}
	w.cond = sync.NewCond(&w.mu)
	w.flushCtx, w.flushCancel = context.WithCancel(context.Background())
	if err != nil {
		w.setError(err)
	}
	return w
}

// newStaged sizes a staging pair to the flush budget.
func newStaged(maxFlushBytes int64) staged {
	return staged{
		data:    make([]byte, 0, max(maxFlushBytes, 4096)),
		pending: make([]pendingEntry, 0, 64),
	}
}

func (w *wal) startSyscall() {
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
		w.waitFlushSlot()
		select {
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
			w.flush()
			return
		}
	}
}

// waitFlushSlot parks the flusher until the in-flight flush completes, so the
// select never spends a wake token on a batch it cannot write yet.
func (w *wal) waitFlushSlot() {
	w.mu.Lock()
	for w.flushInFlight && w.err == nil {
		w.cond.Wait()
	}
	w.mu.Unlock()
}

func (w *wal) updateConfigSyscall(maxFlushBytes int64, maxFlushWait, flushLinger time.Duration) {
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

func (w *wal) pushSyscall(rec []byte, pos int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	n := int64(len(rec))
	seq = w.pushedBytes + n
	w.pushedBytes = seq
	w.stage.data = append(w.stage.data, rec...)
	w.stage.pending = append(w.stage.pending, pendingEntry{pos: pos, len: n})
	w.pendingBytes += n
	w.m.pending.Set(int64(len(w.stage.pending)))

	wake = w.pendingBytes >= w.cfg.MaxFlushBytes
	return seq, wake, nil
}

func (w *wal) pushBatchSyscall(recs [][]byte, positions []int64) (seq int64, wake bool, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.err != nil {
		return 0, false, w.err
	}

	var total int64
	for i, rec := range recs {
		w.stage.data = append(w.stage.data, rec...)
		w.stage.pending = append(w.stage.pending, pendingEntry{pos: positions[i], len: int64(len(rec))})
		total += int64(len(rec))
	}
	w.pendingBytes += total
	w.pushedBytes += total
	seq = w.pushedBytes
	w.m.pending.Set(int64(len(w.stage.pending)))

	wake = w.pendingBytes >= w.cfg.MaxFlushBytes
	return seq, wake, nil
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

// flush hands one staged group to the writer and publishes it once durable.
//
// The flusher parks between batches (waitFlushSlot), so this runs with nothing
// else in flight: producers are released by the completion, which is what makes
// the next group a group commit rather than one flush per producer.
func (w *wal) flush() {
	w.mu.Lock()
	if len(w.stage.data) == 0 || w.err != nil {
		w.mu.Unlock()
		return
	}
	maxBytes := w.cfg.MaxFlushBytes
	batch := w.stage
	w.stage = w.takeStagedLocked(maxBytes)
	w.pendingBytes = 0
	w.flushInFlight = true
	w.m.pending.Set(0)

	// Drain wake signal associated ONLY with the buffers being flushed
	select {
	case <-w.wake:
	default:
	}
	w.mu.Unlock()

	writes, index := w.files.writes(sinkBatch{Records: flushWrites(batch.data, batch.pending)})
	w.m.flushes.Inc()
	start := time.Now()
	done := func(err error) {
		w.m.flushDur.Observe(time.Since(start).Seconds())
		if index != nil {
			index.release(maxIndexKeepBytes)
		}
		w.mu.Lock()
		w.flushInFlight = false
		if err != nil {
			w.setErrorLocked(err)
		} else {
			w.publishedBytes += int64(len(batch.data))
			w.giveStagedLocked(batch, maxBytes)
		}
		w.cond.Broadcast()
		w.mu.Unlock()
	}
	if writerHook != nil {
		writerHook()
	}
	if err := w.writer.Write(w.flushCtx, writes, done); err != nil {
		w.setError(err)
	}
}

// flushWrites renders the staged buffer as one contiguous write when the
// entries tile it, and as per-entry writes at their own offsets otherwise.
func flushWrites(data []byte, entries []pendingEntry) []sinkWrite {
	writes := make([]sinkWrite, len(entries))
	var off int
	contiguous := true
	for i, e := range entries {
		writes[i] = sinkWrite{P: data[off : off+int(e.len)], Off: e.pos}
		off += int(e.len)
		if i > 0 && e.pos != entries[i-1].pos+entries[i-1].len {
			contiguous = false
		}
	}
	if contiguous {
		return []sinkWrite{{P: data, Off: entries[0].pos}}
	}
	return writes
}

// pushAsyncBatchSyscall submits one producer batch to the writer, holding a
// bounded slot for its lifetime and honoring ctx while waiting for one.
func (w *wal) pushAsyncBatchSyscall(ctx context.Context, entries []asyncEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := w.currentError(); err != nil {
		return err
	}
	if err := w.acquireSlot(ctx); err != nil {
		return err
	}

	var total int64
	for _, e := range entries {
		total += e.recLen
	}
	slab := getBuf(int(total))
	positions := make([]int64, len(entries))
	lengths := make([]int64, len(entries))
	var off int
	for i, e := range entries {
		copy(slab.b[off:], e.rec)
		off += len(e.rec)
		positions[i], lengths[i] = e.pos, e.recLen
	}
	writes, index := w.files.writes(sinkBatch{
		Records: []sinkWrite{{P: slab.b, Off: entries[0].pos}},
		Index: sinkIndex{
			Start:     int64(entries[0].offset-entries[0].seg.baseOff) * rawIndexEntrySize,
			Positions: positions,
			Lengths:   lengths,
		},
	})

	encodeKeep := 4 * int(w.cfg.MaxPayload)
	flushBytes := 2 * int(w.cfg.MaxFlushBytes)
	last := entries[len(entries)-1].offset
	start := time.Now()
	done := func(err error) {
		w.m.flushDur.Observe(time.Since(start).Seconds())
		slab.release(flushBytes)
		if index != nil {
			index.release(maxIndexKeepBytes)
		}
		for _, e := range entries {
			if e.buf != nil {
				e.buf.release(encodeKeep)
			}
		}
		<-w.slots
		if err != nil {
			w.setError(err)
			return
		}
		w.mu.Lock()
		if next := last + 1; next > w.asyncNext {
			w.asyncNext = next
		}
		w.cond.Broadcast()
		w.mu.Unlock()
	}

	w.m.flushes.Inc()
	if writerHook != nil {
		writerHook()
	}
	if err := w.writer.Write(w.flushCtx, writes, done); err != nil {
		return err
	}
	return nil
}

// acquireSlot takes one of the async path's bounded slots, counting the wait.
func (w *wal) acquireSlot(ctx context.Context) error {
	select {
	case w.slots <- struct{}{}:
		return nil
	default:
	}
	w.m.blockEvents.Inc()
	w.m.blockedProducers.Add(1)
	defer w.m.blockedProducers.Add(-1)
	select {
	case w.slots <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return ErrQueueClosed
	}
}

// takeStagedLocked hands the flusher a staging pair, reusing a reclaimed one.
func (w *wal) takeStagedLocked(maxBytes int64) staged {
	if n := len(w.free); n > 0 {
		s := w.free[n-1]
		w.free = w.free[:n-1]
		s.data, s.pending = s.data[:0], s.pending[:0]
		return s
	}
	return newStaged(maxBytes)
}

// giveStagedLocked reclaims a completed batch's buffers, dropping oversized ones.
func (w *wal) giveStagedLocked(s staged, maxBytes int64) {
	if int64(cap(s.data)) <= 2*maxBytes {
		s.data = s.data[:0]
	}
	if int64(cap(s.pending)) <= 2*maxBytes/int64(minRecordSize) {
		s.pending = s.pending[:0]
	}
	w.free = append(w.free, s)
}

func (w *wal) stopSyscall() error {
	w.stopOnce.Do(func() {
		close(w.shutdown)
		<-w.done
		if w.writer != nil {
			w.setError(w.writer.Close())
		}
		w.setError(syncData(w.files.log))
		w.flushCancel()
	})
	return w.err
}
