package bench

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/dmunish/mikyu"
)

// runProduce drives the write path: warmup, measured window, then drain.
func runProduce(ctx context.Context, r *runner) error {
	w := r.opts.Workload
	r.beginPhase(PhaseWarmup)
	if w.Warmup > 0 {
		deadline := time.Now().Add(w.Warmup)
		if err := r.produce(ctx, func() bool {
			return r.limitReached(r.written.Load()) || !time.Now().Before(deadline)
		}, false); err != nil {
			return err
		}
	}
	r.endPhase(time.Now())

	r.beginPhase(PhaseMeasure)
	r.beginMeasure()
	if err := r.produce(ctx, func() bool { return r.limitReached(r.written.Load()) }, true); err != nil {
		return err
	}
	r.endMeasure(time.Now())
	r.endPhase(time.Now())

	r.beginPhase(PhaseDrain)
	r.endPhase(time.Now())
	return nil
}

// produce runs one set of producer workers until until reports true or the context ends.
func (r *runner) produce(ctx context.Context, until func() bool, record bool) error {
	w := r.opts.Workload
	var wg sync.WaitGroup
	errCh := make(chan error, w.Workers)

	if r.openLoop() {
		schedCtx, cancel := context.WithCancel(ctx)
		tickets := make(chan time.Time, 256)
		schedDone := make(chan struct{})
		go func() {
			defer close(schedDone)
			r.dispatchRate(schedCtx, tickets, until)
		}()
		for range w.Workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := r.openProduceWorker(schedCtx, tickets, record); err != nil && !errors.Is(err, context.Canceled) {
					errCh <- err
				}
			}()
		}
		wg.Wait()
		cancel()
		<-schedDone
	} else {
		for range w.Workers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := r.closedProduceWorker(ctx, until, record); err != nil && !errors.Is(err, context.Canceled) {
					errCh <- err
				}
			}()
		}
		wg.Wait()
	}

	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}

// closedProduceWorker appends as fast as the engine accepts until until fires.
func (r *runner) closedProduceWorker(ctx context.Context, until func() bool, record bool) error {
	w := r.opts.Workload
	bufs := makePayloadBuffers(w.Batch, w.MsgSize)
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if until() {
			return nil
		}
		start := r.seq.Add(uint64(w.Batch)) - uint64(w.Batch)
		began := time.Now()
		if err := r.appendMessages(ctx, start, bufs); err != nil {
			return err
		}
		if record {
			r.measure.AddMessages(int64(w.Batch))
			r.measure.Record(time.Since(began))
		}
	}
}

// openProduceWorker appends one batch per scheduled ticket, recording corrected and uncorrected latency.
func (r *runner) openProduceWorker(ctx context.Context, tickets <-chan time.Time, record bool) error {
	w := r.opts.Workload
	bufs := makePayloadBuffers(w.Batch, w.MsgSize)
	for scheduled := range tickets {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var obs Observation
		if record {
			obs = r.measure.Start(scheduled)
		}
		start := r.seq.Add(uint64(w.Batch)) - uint64(w.Batch)
		if err := r.appendMessages(ctx, start, bufs); err != nil {
			return err
		}
		if record {
			r.measure.AddMessages(int64(w.Batch))
			r.measure.Finish(obs)
		}
	}
	return nil
}

// appendMessages writes one batch of seeded payloads to a queue.
func (r *runner) appendMessages(ctx context.Context, start uint64, bufs [][]byte) error {
	w := r.opts.Workload
	batchIdx := start / uint64(w.Batch)
	if r.isLog() {
		for i := range w.Batch {
			payloadFor(w.Seed, start+uint64(i), bufs[i])
		}
		var opts []mikyu.AppendOption
		if w.Keys > 0 {
			opts = append(opts, mikyu.WithRecordKey(keyFor(start, w.Keys)))
		}
		q := r.logQueues[int(batchIdx%uint64(len(r.logQueues)))]
		if _, err := q.AppendBatch(ctx, bufs[:w.Batch], opts...); err != nil {
			return err
		}
	} else {
		q := r.taskQueues[int(batchIdx%uint64(len(r.taskQueues)))]
		for i := range w.Batch {
			payloadFor(w.Seed, start+uint64(i), bufs[i])
			if _, err := q.Enqueue(ctx, bufs[i]); err != nil {
				return err
			}
		}
	}
	n := int64(w.Batch) * int64(w.MsgSize)
	r.written.Add(n)
	r.addPhaseBytes(n)
	return nil
}

// dispatchRate paces scheduled send times into the ticket channel at the offered rate.
func (r *runner) dispatchRate(ctx context.Context, tickets chan<- time.Time, until func() bool) {
	defer close(tickets)
	base := time.Now()
	next := base
	for {
		if ctx.Err() != nil || until() {
			return
		}
		rate := r.offeredRate(time.Since(base))
		if rate == 0 {
			if !sleepCtx(ctx, 5*time.Millisecond) {
				return
			}
			continue
		}
		interval := max(time.Second/time.Duration(rate), time.Microsecond)
		for !next.After(time.Now()) {
			select {
			case tickets <- next:
			case <-ctx.Done():
				return
			}
			next = next.Add(interval)
		}
		if wait := time.Until(next); wait > 0 {
			if !sleepCtx(ctx, min(wait, time.Millisecond)) {
				return
			}
		}
	}
}

// offeredRate interpolates the target send rate at elapsed from the workload's schedule.
func (r *runner) offeredRate(elapsed time.Duration) uint64 {
	w := r.opts.Workload
	if len(w.RateSchedule) == 0 {
		return w.Rate
	}
	points := w.RateSchedule
	if elapsed <= points[0].At {
		return points[0].Rate
	}
	for i := 1; i < len(points); i++ {
		if elapsed >= points[i].At {
			continue
		}
		span := points[i].At - points[i-1].At
		if span <= 0 {
			return points[i].Rate
		}
		frac := float64(elapsed-points[i-1].At) / float64(span)
		v := float64(points[i-1].Rate) + frac*float64(int64(points[i].Rate)-int64(points[i-1].Rate))
		if v <= 0 {
			return 0
		}
		return uint64(v)
	}
	return points[len(points)-1].Rate
}

// makePayloadBuffers allocates one reusable buffer per message in a batch.
func makePayloadBuffers(batch, size int) [][]byte {
	bufs := make([][]byte, batch)
	for i := range bufs {
		bufs[i] = make([]byte, size)
	}
	return bufs
}

// keyFor renders the compaction key for a record.
func keyFor(offset uint64, keys int) []byte {
	return fmt.Appendf(nil, "key-%d", offset%uint64(keys))
}

// sleepCtx waits for d or until the context ends, reporting whether the wait completed.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}
