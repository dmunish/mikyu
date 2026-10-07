package engine

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// ringSlotCapacity bounds the number of outstanding async batches in slots sized for the worst-case payload.
func ringSlotCapacity(cfg Config) int {
	return max(1, int(cfg.MaxFlushBytes/max(int64(1), cfg.MaxPayload)))
}

// newRingWAL builds the ring WAL: producers submit every batch directly to the
// shared ring and the reaper completes them; there is no flusher goroutine.
func newRingWAL(seg *segment, cfg Config, startOffset uint64, ring *uringRing, scopes ...*metrics.Scope) *wal {
	w := &wal{
		cfg:       cfg,
		sink:      newRingSink(seg.logFile, seg.idxFile, !cfg.Async(), ring),
		ring:      ring,
		asyncNext: startOffset,
		slots:     make(chan struct{}, ringSlotCapacity(cfg)),
		completed: make(map[uint64]int64),
		shutdown:  make(chan struct{}),
		done:      make(chan struct{}),
		m:         newWALMetrics(scopeOf(scopes)),
	}
	w.cond = sync.NewCond(&w.mu)
	return w
}

func (w *wal) startRing() {}

func (w *wal) updateConfigRing(maxFlushBytes int64, maxFlushWait, flushLinger time.Duration) {
	w.mu.Lock()
	w.cfg.MaxFlushBytes = maxFlushBytes
	w.cfg.MaxFlushWait = maxFlushWait
	w.cfg.FlushLinger = flushLinger
	w.mu.Unlock()
}

func (w *wal) pushRing(rec []byte, pos int64) (int64, bool, error) {
	return w.pushBatchRing([][]byte{rec}, []int64{pos}, -1)
}

// pushBatchRing submits one sync batch to the ring under a single tag and
// returns a byte sequence wait() can block on until the completion watermark
// covers it. The records are copied into a slab, so the caller may recycle
// its buffers as soon as this returns.
func (w *wal) pushBatchRing(recs [][]byte, positions []int64, idxStart int64) (int64, bool, error) {
	if len(recs) == 0 {
		return 0, false, nil
	}
	w.mu.Lock()
	if w.err != nil {
		err := w.err
		w.mu.Unlock()
		return 0, false, err
	}
	var total int64
	for _, rec := range recs {
		total += int64(len(rec))
	}
	w.submitSeq++
	s := w.submitSeq
	w.pushedBytes += total
	seq := w.pushedBytes
	wake := total >= w.cfg.MaxFlushBytes
	maxFlushBytes := w.cfg.MaxFlushBytes
	w.mu.Unlock()

	slab := getBuf(int(total))
	var off int
	for _, rec := range recs {
		copy(slab.b[off:], rec)
		off += len(rec)
	}
	batch := sinkBatch{Records: []sinkWrite{{P: slab.b, Off: positions[0]}}}
	if idxStart >= 0 {
		lengths := make([]int64, len(recs))
		for i, rec := range recs {
			lengths[i] = int64(len(rec))
		}
		batch.Index = sinkIndex{Start: idxStart, Positions: positions, Lengths: lengths}
	}
	release := func() { slab.release(2 * int(maxFlushBytes)) }
	w.inflightWG.Add(1)
	if err := w.sink.Write(batch, w.ringDone(s, total, 0, release)); err != nil {
		w.inflightWG.Done()
		release()
		w.setError(err)
		return 0, false, err
	}
	w.m.flushes.Inc()
	return seq, wake, nil
}

// pushAsyncBatchRing submits one async batch to the ring and returns
// immediately; the reaper's completion callback recycles the buffers,
// advances asyncNext, and releases the producer's backpressure slot.
func (w *wal) pushAsyncBatchRing(ctx context.Context, entries []asyncEntry, idxStart int64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case w.slots <- struct{}{}:
	default:
	}
	w.m.blockEvents.Inc()
	w.m.blockedProducers.Add(1)
	defer w.m.blockedProducers.Add(-1)
	select {
	case w.slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	case <-w.shutdown:
		return ErrQueueClosed
	}

	var total int64
	for _, e := range entries {
		total += e.recLen
	}
	slab := getBuf(int(total))
	var off int
	positions := make([]int64, len(entries))
	lengths := make([]int64, len(entries))
	var bufs []*pooledBuf
	for i, e := range entries {
		copy(slab.b[off:], e.rec)
		off += len(e.rec)
		positions[i], lengths[i] = e.pos, e.recLen
		if e.buf != nil {
			bufs = append(bufs, e.buf)
		}
	}
	batch := sinkBatch{
		Records: []sinkWrite{{P: slab.b, Off: entries[0].pos}},
		Index:   sinkIndex{Start: idxStart, Positions: positions, Lengths: lengths},
	}

	w.mu.Lock()
	w.submitSeq++
	s := w.submitSeq
	w.pushedBytes += total
	lastOffset := entries[len(entries)-1].offset
	maxFlushBytes := w.cfg.MaxFlushBytes
	maxPayload := w.cfg.MaxPayload
	w.mu.Unlock()

	encodeKeep := 4 * int(maxPayload)
	release := func() {
		slab.release(2 * int(maxFlushBytes))
		for _, b := range bufs {
			b.release(encodeKeep)
		}
	}
	w.inflightWG.Add(1)
	if err := w.sink.Write(batch, w.ringDone(s, total, lastOffset, release)); err != nil {
		w.inflightWG.Done()
		release()
		return err
	}
	w.m.flushes.Inc()
	return nil
}

// ringDone builds the completion callback for one ring submission: it
// recycles the batch's buffers, advances the durable watermark over
// out-of-order completions, and wakes waiters.
func (w *wal) ringDone(s uint64, total int64, lastOffset uint64, release func()) sinkDone {
	return func(err error) {
		defer w.inflightWG.Done()
		if release != nil {
			release()
		}
		w.mu.Lock()
		defer w.mu.Unlock()
		if err != nil {
			if w.err == nil {
				w.err = err
			}
			w.cond.Broadcast()
			return
		}
		if last := lastOffset + 1; lastOffset > 0 && last > w.asyncNext {
			w.asyncNext = last
		}
		w.completed[s] = total
		for {
			t, ok := w.completed[w.watermark+1]
			if !ok {
				break
			}
			w.publishedBytes += t
			delete(w.completed, w.watermark+1)
			w.watermark++
		}
		w.cond.Broadcast()
	}
}

func (w *wal) stopRing() error {
	w.stopOnce.Do(func() {
		close(w.shutdown)
		w.inflightWG.Wait()
		w.setError(w.sink.Sync())
		w.setError(w.sink.Close())
		close(w.done)
	})
	return w.err
}
