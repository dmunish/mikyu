package bench

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/bench/result"
)

// observer samples client, system, and host observations once per second and streams them to the envelope.
type observer struct {
	client mikyu.Client
	env    *envelope
	m      *Measure
	phase  func() Phase
	names  []string
	kind   string
	stderr io.Writer

	start  time.Time
	lastAt time.Time
	calls  *Histogram

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	mu     sync.Mutex
}

// newObserver builds an observer over one run's client, envelope, and queue set.
func newObserver(client mikyu.Client, env *envelope, m *Measure, phase func() Phase, names []string, kind string, stderr io.Writer) *observer {
	return &observer{client: client, env: env, m: m, phase: phase, names: names, kind: kind, stderr: stderr, calls: &Histogram{}}
}

// Start launches the sampling goroutine; Stop ends it.
func (o *observer) Start(ctx context.Context) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.ctx, o.cancel = context.WithCancel(ctx)
	o.done = make(chan struct{})
	o.start = time.Now()
	o.lastAt = o.start
	go o.run()
}

// Stop ends the sampling goroutine and records how long its client calls took.
func (o *observer) Stop() {
	o.mu.Lock()
	cancel, done := o.cancel, o.done
	o.mu.Unlock()
	if cancel == nil {
		return
	}
	cancel()
	<-done
	if o.calls.Len() > 0 {
		o.env.AddNote(fmt.Sprintf("observer metrics call p99: %s", o.calls.Percentile(0.99)))
	}
}

// run samples on a one-second cadence until the context ends.
func (o *observer) run() {
	defer close(o.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-o.ctx.Done():
			return
		case <-ticker.C:
			o.tick()
		}
	}
}

// tick appends one observation row.
func (o *observer) tick() {
	now := time.Now()
	began := time.Now()
	snapshot, err := o.client.Metrics(o.ctx)
	o.calls.Record(time.Since(began))
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			fmt.Fprintf(o.stderr, "bench observer: metrics: %v\n", err)
		}
		return
	}
	row := result.SeriesRow{
		T:      now.Sub(o.start).Seconds(),
		Phase:  string(o.phase()),
		Client: o.clientCols(now),
		System: systemCols(snapshot, o.names, o.kind),
		Host:   hostCols(readHostStats()),
	}
	if err := o.env.AppendRow(row); err != nil {
		fmt.Fprintf(o.stderr, "bench observer: series: %v\n", err)
	}
}

// clientCols computes the throughput and latency deltas since the previous tick.
func (o *observer) clientCols(now time.Time) result.ClientCols {
	delta := o.m.SnapshotSince(o.lastAt)
	elapsed := now.Sub(o.lastAt).Seconds()
	o.lastAt = now
	throughput := 0.0
	if elapsed > 0 {
		throughput = float64(delta.Messages) / elapsed
	}
	return result.ClientCols{
		Throughput: throughput,
		P50NS:      delta.LatencyUncorrected.P50,
		P90NS:      delta.LatencyUncorrected.P90,
		P99NS:      delta.LatencyUncorrected.P99,
	}
}

// systemCols aggregates the run's queues from one snapshot.
func systemCols(snapshot mikyu.Metrics, names []string, kind string) result.SystemCols {
	var cols result.SystemCols
	for _, q := range snapshot.Queues {
		if !slices.Contains(names, q.Name) {
			continue
		}
		if kind == KindTask {
			cols.QueueDepth += q.Tasks.Active
		} else {
			cols.QueueDepth += q.LastOffset() - q.MinOffset()
		}
		cols.Segments += q.Segments()
		cols.SizeBytes += q.SizeBytes()
		cols.CacheHits += q.Counters["cache_hits"]
		cols.CacheMisses += q.Counters["cache_misses"]
		cols.WALChannelFill = max(cols.WALChannelFill, q.Backpressure.WALChannelFill)
		cols.WALByteFill = max(cols.WALByteFill, q.Backpressure.WALByteFill)
	}
	cols.CacheBytes = snapshot.Storage.CacheBytes
	return cols
}

// hostCols converts the host reading into the series shape.
func hostCols(stats HostStats) result.HostCols {
	return result.HostCols{
		RSSBytes:   stats.RSSBytes,
		CPUPercent: stats.CPUPercent,
		OpenFDs:    stats.OpenFDs,
		Threads:    stats.Threads,
	}
}
