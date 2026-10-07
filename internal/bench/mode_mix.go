package bench

import (
	"context"
	"errors"
	"sync"
	"time"
)

// runMix drives producers and consumers concurrently: a measured window, then drain.
func runMix(ctx context.Context, r *runner) error {
	r.beginPhase(PhaseMeasure)
	r.beginMeasure()
	if err := r.mix(ctx, func() bool { return r.limitReached(r.written.Load()) }); err != nil {
		return err
	}
	r.endMeasure(time.Now())
	r.endPhase(time.Now())

	r.beginPhase(PhaseDrain)
	r.endPhase(time.Now())
	return nil
}

// mix runs the producer and consumer worker pools against one shared bound.
func (r *runner) mix(ctx context.Context, until func() bool) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := r.produce(runCtx, until); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	go func() {
		defer wg.Done()
		if err := r.consume(runCtx, until, false); err != nil && !errors.Is(err, context.Canceled) {
			errCh <- err
		}
	}()
	wg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return ctx.Err()
	}
}
