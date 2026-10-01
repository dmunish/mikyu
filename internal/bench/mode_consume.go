package bench

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu"
)

// runConsume drives the read path: optional populate, warmup, measured window, then drain.
func runConsume(ctx context.Context, r *runner) error {
	w := r.opts.Workload
	if w.Preload > 0 {
		r.beginPhase(PhasePopulate)
		if err := r.populate(ctx); err != nil {
			return err
		}
		r.endPhase(time.Now())
	} else {
		fmt.Fprintln(r.opts.Stderr, "note: consuming from an empty queue; use --preload to measure against a backlog")
	}

	r.beginPhase(PhaseWarmup)
	if w.Warmup > 0 {
		deadline := time.Now().Add(w.Warmup)
		if err := r.consume(ctx, func() bool {
			return r.limitReached(r.consumed.Load()) || !time.Now().Before(deadline)
		}, false, true); err != nil {
			return err
		}
	}
	r.endPhase(time.Now())

	r.beginPhase(PhaseMeasure)
	r.beginMeasure()
	if err := r.consume(ctx, func() bool { return r.limitReached(r.consumed.Load()) }, true, true); err != nil {
		return err
	}
	r.endMeasure(time.Now())
	r.endPhase(time.Now())

	r.beginPhase(PhaseDrain)
	r.endPhase(time.Now())
	return nil
}

// populate writes the preload backlog as seeded payloads, keeping each queue's offsets aligned.
func (r *runner) populate(ctx context.Context) error {
	w := r.opts.Workload
	total := w.Preload / int64(w.MsgSize)
	if total <= 0 {
		return nil
	}
	buf := make([]byte, w.MsgSize)
	next := make([]uint64, len(r.queueNames))
	for i := int64(0); i < total; i++ {
		qi := int(i) % len(r.queueNames)
		payloadFor(w.Seed, next[qi], buf)
		payload := slices.Clone(buf)
		if r.isLog() {
			if _, err := r.logQueues[qi].Append(ctx, payload); err != nil {
				return err
			}
		} else if _, err := r.taskQueues[qi].Enqueue(ctx, payload); err != nil {
			return err
		}
		next[qi]++
		if i%1024 == 0 {
			emitProgress(r.opts, ProgressEvent{Kind: ProgressTick, Phase: PhasePopulate, Bytes: i * int64(w.MsgSize), Total: w.Preload})
		}
	}
	emitProgress(r.opts, ProgressEvent{Kind: ProgressEnd, Phase: PhasePopulate, Bytes: total * int64(w.MsgSize), Total: w.Preload})
	return nil
}

// consume dispatches to the log or task reader.
func (r *runner) consume(ctx context.Context, until func() bool, record, stopWhenIdle bool) error {
	if r.isLog() {
		return r.consumeLog(ctx, until, record, stopWhenIdle)
	}
	return r.consumeTask(ctx, until, record)
}

// queueCursor hands out distinct read ranges for one log queue.
type queueCursor struct {
	q    mikyu.LogQueue
	mu   sync.Mutex
	next uint64
}

// claim reserves the next batch if the queue still has records.
func (c *queueCursor) claim(batch int, total uint64) (uint64, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.next >= total {
		return 0, false
	}
	start := c.next
	c.next += uint64(batch)
	return start, true
}

// consumeLog reads every queue with a shared cursor so workers never duplicate a range.
func (r *runner) consumeLog(ctx context.Context, until func() bool, record, stopWhenIdle bool) error {
	w := r.opts.Workload
	r.ensureCursors(ctx)
	var idle atomic.Int64
	combined := func() bool {
		return until() || (stopWhenIdle && idle.Load() >= int64(w.Workers))
	}
	var wg sync.WaitGroup
	errCh := make(chan error, w.Workers)
	for range w.Workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := r.logConsumeWorker(ctx, combined, record, &idle); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

// logConsumeWorker claims and reads batches until the run's bound fires.
func (r *runner) logConsumeWorker(ctx context.Context, until func() bool, record bool, idle *atomic.Int64) error {
	w := r.opts.Workload
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if until() {
			return nil
		}
		cursor := r.pickCursor()
		total, err := cursor.q.LastOffset(ctx)
		if err != nil {
			return err
		}
		start, ok := cursor.claim(w.Batch, total)
		if !ok {
			idle.Add(1)
			if !sleepCtx(ctx, 5*time.Millisecond) {
				return ctx.Err()
			}
			continue
		}
		began := time.Now()
		records, err := cursor.q.ReadBatch(ctx, start, w.Batch)
		if err != nil {
			if errors.Is(err, mikyu.ErrOffsetNotFound) || errors.Is(err, mikyu.ErrOffsetTruncated) {
				idle.Add(1)
				continue
			}
			return err
		}
		idle.Store(0)
		for _, rec := range records {
			if r.opts.Verify && record {
				if verr := verifyPayload(w.Seed, rec.Offset, rec.Payload); verr != nil {
					return verr
				}
			}
			r.consumed.Add(int64(len(rec.Payload)))
			r.addPhaseBytes(int64(len(rec.Payload)))
		}
		if record && len(records) > 0 {
			r.measure.AddMessages(int64(len(records)))
			r.measure.Record(time.Since(began))
		}
	}
}

// ensureCursors builds one cursor per log queue, anchored at its oldest retained offset.
func (r *runner) ensureCursors(ctx context.Context) {
	if r.cursors != nil {
		return
	}
	r.cursors = make([]*queueCursor, len(r.logQueues))
	for i, q := range r.logQueues {
		cursor := &queueCursor{q: q}
		if minOffset, err := q.MinOffset(ctx); err == nil {
			cursor.next = minOffset
		}
		r.cursors[i] = cursor
	}
}

// pickCursor rotates through the run's cursors to spread reads across queues.
func (r *runner) pickCursor() *queueCursor {
	idx := int(r.cursorPick.Add(1)-1) % len(r.cursors)
	return r.cursors[idx]
}

// consumeTask dequeues, verifies, and acknowledges tasks from every queue.
func (r *runner) consumeTask(ctx context.Context, until func() bool, record bool) error {
	w := r.opts.Workload
	var wg sync.WaitGroup
	errCh := make(chan error, w.Workers)
	for i := range w.Workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q := r.taskQueues[i%len(r.taskQueues)]
			if err := r.taskConsumeWorker(ctx, q, until, record); err != nil && !errors.Is(err, context.Canceled) {
				errCh <- err
			}
		}(i)
	}
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

// taskConsumeWorker dequeues one task at a time and acks or nacks it.
func (r *runner) taskConsumeWorker(ctx context.Context, q mikyu.TaskQueue, until func() bool, record bool) error {
	w := r.opts.Workload
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if until() {
			return nil
		}
		task, err := q.TryDequeue(ctx)
		switch {
		case errors.Is(err, mikyu.ErrTaskNotAvailable):
			if !sleepCtx(ctx, 5*time.Millisecond) {
				return ctx.Err()
			}
			continue
		case err != nil:
			return err
		}
		began := time.Now()
		if r.opts.Verify && record {
			if verr := verifyPayload(w.Seed, task.Offset, task.Payload); verr != nil {
				return verr
			}
		}
		if r.ackDecision(task.Offset) {
			if err := q.Ack(ctx, task); err != nil {
				return err
			}
		} else if err := q.Nack(ctx, task); err != nil {
			return err
		}
		r.consumed.Add(int64(len(task.Payload)))
		r.addPhaseBytes(int64(len(task.Payload)))
		if record {
			r.measure.AddMessages(1)
			r.measure.Record(time.Since(began))
		}
	}
}

// ackDecision chooses Ack over Nack deterministically from the seed and task offset.
func (r *runner) ackDecision(offset uint64) bool {
	rate := r.opts.Workload.AckRate
	switch {
	case rate >= 1:
		return true
	case rate <= 0:
		return false
	}
	rng := rand.New(rand.NewPCG(uint64(r.opts.Workload.Seed), offset))
	return float64(rng.Uint64()%1000)/1000 < rate
}
