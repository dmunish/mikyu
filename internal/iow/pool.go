package iow

import (
	"context"
	"os"
	"runtime"
	"slices"
	"sync"
)

// syncData is the platform fsync, injectable so tests can count or fail it.
var syncData = syncDataDefault

// writeAt is the platform write, injectable so tests can block, delay, or fail it.
var writeAt = func(f *os.File, buf []byte, off int64) (int, error) { return f.WriteAt(buf, off) }

// job is one batch awaiting execution.
type job struct {
	seq    uint64
	writes []Write
	done   func(error)
}

// pool writes batches through a bounded worker pool and delivers done in submission order.
type pool struct {
	files     []*os.File
	syncFiles []*os.File
	durable   bool

	jobs    chan *job
	workers int

	mu     sync.Mutex
	cond   *sync.Cond
	closed bool
	order  *order
	err    error

	closeOnce sync.Once
	closing   chan struct{}
	submitWG  sync.WaitGroup
	wg        sync.WaitGroup
}

// newPool builds a pool sized by Options, defaulting workers and queue depth to GOMAXPROCS.
func newPool(files []*os.File, opts Options) *pool {
	workers := opts.Workers
	if workers <= 0 {
		workers = runtime.GOMAXPROCS(0)
	}
	p := &pool{
		files:     files,
		syncFiles: opts.SyncFiles,
		durable:   opts.Durability == DataSync,
		jobs:      make(chan *job, outstandingDepth(opts)),
		workers:   workers,
		order:     newOrder(),
		closing:   make(chan struct{}),
	}
	p.cond = sync.NewCond(&p.mu)
	for range p.workers {
		p.wg.Add(1)
		go p.run()
	}
	return p
}

// Write hands one batch to a worker, or completes it inline when closed, failed, or cancelled.
func (p *pool) Write(ctx context.Context, batch []Write, done func(error)) error {
	p.mu.Lock()
	err := p.err
	if err == nil && p.closed {
		err = ErrClosed
	}
	if err == nil {
		err = ctx.Err()
	}
	seq := p.order.reserve()
	if err != nil {
		p.mu.Unlock()
		p.order.settle(seq, err, done)
		return err
	}
	p.submitWG.Add(1)
	p.mu.Unlock()

	j := &job{seq: seq, writes: batch, done: done}
	select {
	case p.jobs <- j:
		p.submitWG.Done()
		return nil
	case <-p.closing:
		p.submitWG.Done()
		p.order.settle(seq, ErrClosed, done)
		return ErrClosed
	case <-ctx.Done():
		p.submitWG.Done()
		p.order.settle(seq, ctx.Err(), done)
		return ctx.Err()
	}
}

// Sync waits for the submitted prefix to drain, then fsyncs every owned file.
func (p *pool) Sync() error {
	p.mu.Lock()
	err := p.err
	p.mu.Unlock()
	if err != nil {
		return err
	}
	p.order.waitDrained()
	for _, f := range p.files {
		if serr := syncData(f); serr != nil {
			return serr
		}
	}
	return nil
}

// Close drains accepted batches, fsyncs, and closes the files exactly once.
func (p *pool) Close() error {
	p.closeOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		p.mu.Unlock()
		p.submitWG.Wait()
		close(p.closing)
		p.wg.Wait()

		p.mu.Lock()
		err := p.err
		p.mu.Unlock()
		if err == nil {
			for _, f := range p.files {
				if serr := syncData(f); serr != nil {
					err = serr
					break
				}
			}
		}
		for _, f := range p.files {
			if cerr := f.Close(); cerr != nil && err == nil {
				err = cerr
			}
		}
		p.mu.Lock()
		if p.err == nil {
			p.err = err
		}
		p.mu.Unlock()
	})
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

// run executes jobs until close, then drains whatever is already queued.
func (p *pool) run() {
	defer p.wg.Done()
	for {
		select {
		case j := <-p.jobs:
			p.order.settle(j.seq, p.execute(j), j.done)
		case <-p.closing:
			for {
				select {
				case j := <-p.jobs:
					p.order.settle(j.seq, p.execute(j), j.done)
				default:
					return
				}
			}
		}
	}
}

// execute writes every buffer in the batch, then fsyncs the sync targets under DataSync.
func (p *pool) execute(j *job) error {
	if err := p.currentError(); err != nil {
		return err
	}
	for _, w := range j.writes {
		if _, err := writeAt(w.F, w.Buf, w.Off); err != nil {
			p.setError(err)
			return err
		}
	}
	if !p.durable {
		return nil
	}
	for _, f := range p.syncTargets(j.writes) {
		if err := syncData(f); err != nil {
			p.setError(err)
			return err
		}
	}
	return nil
}

// syncTargets lists the files one batch must fsync, deduplicated in first-seen order.
func (p *pool) syncTargets(writes []Write) []*os.File {
	if len(p.syncFiles) > 0 {
		return p.syncFiles
	}
	targets := make([]*os.File, 0, len(writes))
	for _, w := range writes {
		if w.F != nil && !slices.Contains(targets, w.F) {
			targets = append(targets, w.F)
		}
	}
	return targets
}

// setError records the first fatal batch error.
func (p *pool) setError(err error) {
	p.mu.Lock()
	if p.err == nil {
		p.err = err
	}
	p.mu.Unlock()
}

// currentError reports the recorded fatal error, if any.
func (p *pool) currentError() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}
