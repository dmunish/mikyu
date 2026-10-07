package engine

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// newSyscallWAL builds the syscall WAL: staged sync batches behind one
// flusher goroutine, async batches handed to the sink's drain loop.
func newSyscallWAL(seg *segment, cfg Config, startOffset uint64, scopes ...*metrics.Scope) *wal {
	flushBytes := max(cfg.MaxFlushBytes, 4096)
	w := &wal{
		cfg:           cfg,
		sink:          newSink(seg.logFile, seg.idxFile, cfg, nil),
		asyncNext:     startOffset,
		buf:           make([]byte, 0, flushBytes),
		pending:       make([]pendingEntry, 0, 64),
		swapBuf:       make([]byte, 0, flushBytes),
		swapPending:   make([]pendingEntry, 0, 64),
		asyncChan:     make(chan []asyncEntry, max(1, int(cfg.MaxFlushBytes)/4096)),
		wake:          make(chan struct{}, 1),
		configChanged: make(chan struct{}, 1),
		shutdown:      make(chan struct{}),
		done:          make(chan struct{}),
		m:             newWALMetrics(scopeOf(scopes)),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
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
		select {
		case entries := <-w.asyncChan:
			w.drainAndFlushAsync(entries)
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

func (w *wal) shutdownFlush() {
	w.drainRemainingAsync()
	w.flush()
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
	w.buf = append(w.buf, rec...)
	w.pending = append(w.pending, pendingEntry{pos: pos, len: n})
	w.pendingBytes += n
	w.m.pending.Set(int64(len(w.pending)))

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

// pushAsyncBatchSyscall stages one producer batch on the async path as a single channel slot, blocking while the channel is full and honoring ctx.
func (w *wal) pushAsyncBatchSyscall(ctx context.Context, entries []asyncEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case w.asyncChan <- entries:
		return nil
	default:
	}
	w.m.blockEvents.Inc()
	w.m.blockedProducers.Add(1)
	defer w.m.blockedProducers.Add(-1)
	select {
	case w.asyncChan <- entries:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return ErrQueueClosed
	}
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
	maxBytes := w.cfg.MaxFlushBytes

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
	writeErr := w.sink.Write(sinkBatch{Records: flushWrites(data, entries)}, nil)
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())

	w.mu.Lock()
	if writeErr != nil {
		w.err = writeErr
	} else {
		w.publishedBytes += int64(len(data))
	}

	// Reclaim used buffer capacity for next swap cycle, trimming oversized buffers
	if int64(cap(data)) > 2*maxBytes {
		w.swapBuf = make([]byte, 0, maxBytes)
	} else {
		w.swapBuf = data[:0]
	}
	if int64(cap(entries)) > 2*maxBytes/int64(minRecordSize) {
		w.swapPending = make([]pendingEntry, 0, 64)
	} else {
		w.swapPending = entries[:0]
	}
	w.m.pending.Set(int64(len(w.pending)))

	w.cond.Broadcast()
	w.mu.Unlock()
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

func (w *wal) drainAndFlushAsync(first []asyncEntry) {
	w.mu.Lock()
	maxBytes := w.cfg.MaxFlushBytes
	w.mu.Unlock()

	entries := first
	var total int64
	for _, e := range first {
		total += e.recLen
	}

	for total < maxBytes {
		select {
		case more := <-w.asyncChan:
			entries = append(entries, more...)
			for _, e := range more {
				total += e.recLen
			}
		default:
			goto WRITE
		}
	}

WRITE:
	w.mu.Lock()
	failed := w.err
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
		if err := w.writeRun(entries[i:j]); err != nil {
			w.setError(err)
			return
		}
		i = j
	}
	w.m.flushes.Inc()
	w.m.flushDur.Observe(time.Since(start).Seconds())
}

// writeRun submits one contiguous same-segment run to the sink as a single
// log write with the run's index entries alongside it; the sink's completion
// advances asyncNext and recycles the run's buffers.
func (w *wal) writeRun(run []asyncEntry) error {
	if writerHook != nil {
		writerHook()
	}
	data := run[0].rec
	var slab *pooledBuf
	if len(run) > 1 {
		var totalLen int64
		for _, e := range run {
			totalLen += e.recLen
		}
		slab = getBuf(int(totalLen))
		var off int
		for _, e := range run {
			copy(slab.b[off:], e.rec)
			off += len(e.rec)
		}
		data = slab.b
	}
	batch := sinkBatch{
		Records: []sinkWrite{{P: data, Off: run[0].pos}},
		Index:   asyncIndex(run),
	}
	encodeKeep := 4 * int(w.cfg.MaxPayload)
	done := func(err error) {
		if slab != nil {
			slab.release(2 * int(w.cfg.MaxFlushBytes))
		}
		for _, e := range run {
			if e.buf != nil {
				e.buf.release(encodeKeep)
			}
		}
		if err != nil {
			w.setError(err)
			return
		}
		w.mu.Lock()
		if last := run[len(run)-1].offset + 1; last > w.asyncNext {
			w.asyncNext = last
		}
		w.cond.Broadcast()
		w.mu.Unlock()
	}
	return w.sink.Write(batch, done)
}

// asyncIndex renders one async run's index entries for the sink.
func asyncIndex(run []asyncEntry) sinkIndex {
	positions := make([]int64, len(run))
	lengths := make([]int64, len(run))
	for i, e := range run {
		positions[i], lengths[i] = e.pos, e.recLen
	}
	return sinkIndex{
		Start:     int64(run[0].offset-run[0].seg.baseOff) * rawIndexEntrySize,
		Positions: positions,
		Lengths:   lengths,
	}
}

func (w *wal) drainRemainingAsync() {
	for {
		select {
		case entries := <-w.asyncChan:
			w.drainAndFlushAsync(entries)
		default:
			return
		}
	}
}

func (w *wal) stopSyscall() error {
	w.stopOnce.Do(func() {
		close(w.shutdown)
		<-w.done
		w.setError(w.sink.Close())
		w.setError(w.sink.Sync())
	})
	return w.err
}
