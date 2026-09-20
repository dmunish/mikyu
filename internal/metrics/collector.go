package metrics

import (
	"context"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Config controls collector cadence, retention, and histogram capture.
type Config struct {
	Interval   time.Duration
	History    int
	Histograms bool
	Disabled   bool
}

// Collector periodically materializes snapshots from a Registry and its sources.
type Collector struct {
	reg     *Registry
	cfg     Config
	sources []Source

	latest  atomic.Pointer[Snapshot]
	history *RingBuffer[Snapshot]

	mu       sync.Mutex
	subs     map[int]*subscriber
	nextSub  int
	started  bool
	stopped  bool
	stopOnce sync.Once
	stop     chan struct{}
	wg       sync.WaitGroup
}

type subscriber struct {
	ch      chan Snapshot
	dropped bool
}

// New builds a collector over reg with the given tick cadence.
func New(reg *Registry, cfg Config, sources ...Source) *Collector {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.History < 1 {
		cfg.History = 1
	}
	return &Collector{
		reg:     reg,
		cfg:     cfg,
		sources: sources,
		history: NewRingBuffer[Snapshot](cfg.History),
		subs:    make(map[int]*subscriber),
		stop:    make(chan struct{}),
	}
}

// Registry returns the collector's registry.
func (c *Collector) Registry() *Registry { return c.reg }

// Config returns the collector's configuration.
func (c *Collector) Config() Config { return c.cfg }

// Start launches the ticker goroutine. It is idempotent.
func (c *Collector) Start(ctx context.Context) {
	if c.cfg.Disabled {
		return
	}
	c.mu.Lock()
	if c.started || c.stopped {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()
	c.wg.Add(1)
	go c.run(ctx)
}

// Stop terminates the ticker goroutine. It is idempotent and safe before Start.
func (c *Collector) Stop() {
	c.stopOnce.Do(func() {
		c.mu.Lock()
		c.stopped = true
		started := c.started
		c.mu.Unlock()
		close(c.stop)
		if started {
			c.wg.Wait()
		}
		c.mu.Lock()
		for id, sub := range c.subs {
			close(sub.ch)
			delete(c.subs, id)
		}
		c.mu.Unlock()
	})
}

// Snapshot materializes a fresh snapshot and records it as the latest.
func (c *Collector) Snapshot(ctx context.Context) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	snap := c.build(ctx)
	c.latest.Store(&snap)
	return snap, nil
}

// History returns buffered tick snapshots, oldest first.
func (c *Collector) History() []Snapshot { return c.history.Snapshot() }

// Subscribe returns a channel of snapshots and a cancel function.
func (c *Collector) Subscribe() (<-chan Snapshot, func()) {
	c.mu.Lock()
	if c.stopped || c.cfg.Disabled {
		ch := make(chan Snapshot)
		close(ch)
		c.mu.Unlock()
		return ch, func() {}
	}
	id := c.nextSub
	c.nextSub++
	ch := make(chan Snapshot, 1)
	c.subs[id] = &subscriber{ch: ch}
	c.mu.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			c.mu.Lock()
			if sub, ok := c.subs[id]; ok {
				delete(c.subs, id)
				close(sub.ch)
			}
			c.mu.Unlock()
		})
	}
	return ch, cancel
}

func (c *Collector) run(ctx context.Context) {
	defer c.wg.Done()
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	c.tick(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.stop:
			return
		case <-ticker.C:
			c.tick(ctx)
		}
	}
}

func (c *Collector) tick(ctx context.Context) {
	snap := c.build(ctx)
	c.history.Push(snap)
	c.latest.Store(&snap)
	c.fanout(snap)
}

func (c *Collector) fanout(snap Snapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, sub := range c.subs {
		select {
		case sub.ch <- snap:
		default:
			if !sub.dropped {
				sub.dropped = true
				slog.Debug("metrics: dropping snapshots for slow subscriber")
			}
		}
	}
}

func (c *Collector) build(ctx context.Context) Snapshot {
	snap := Snapshot{Disabled: c.cfg.Disabled, Timestamp: time.Now()}
	for _, src := range c.sources {
		src.Collect(ctx, &snap)
	}
	c.mergeScopes(&snap)
	if prev := c.latest.Load(); prev != nil {
		computeRates(prev, &snap)
	}
	slices.SortFunc(snap.Queues, func(a, b QueueView) int {
		return strings.Compare(a.Name, b.Name)
	})
	snap.Totals = totalize(snap)
	return snap
}

func (c *Collector) mergeScopes(snap *Snapshot) {
	if c.reg == nil {
		return
	}
	index := make(map[string]int, len(snap.Queues))
	for i := range snap.Queues {
		index[snap.Queues[i].Name] = i
	}
	for scope := range c.reg.Scopes() {
		if scope == nil {
			continue
		}
		counters, gauges, histograms := scope.read()
		pos, ok := index[scope.Name()]
		if !ok {
			snap.Queues = append(snap.Queues, QueueView{Name: scope.Name(), Kind: "internal"})
			pos = len(snap.Queues) - 1
			index[scope.Name()] = pos
		}
		qv := &snap.Queues[pos]
		if qv.Kind == "" {
			qv.Kind = "internal"
		}
		// Sources and scopes both contribute instruments to the same queue view,
		// so merge rather than replace: a source's gauges must survive.
		mergeInt64(&qv.Counters, counters)
		mergeInt64(&qv.Gauges, gauges)
		if c.cfg.Histograms {
			mergeHistograms(&qv.Histograms, histograms)
		}
	}
}

func mergeInt64(dst *map[string]int64, src map[string]int64) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]int64, len(src))
	}
	maps.Copy((*dst), src)
}

func mergeHistograms(dst *map[string]HistogramSnapshot, src map[string]HistogramSnapshot) {
	if len(src) == 0 {
		return
	}
	if *dst == nil {
		*dst = make(map[string]HistogramSnapshot, len(src))
	}
	maps.Copy((*dst), src)
}

func computeRates(prev *Snapshot, next *Snapshot) {
	elapsed := next.Timestamp.Sub(prev.Timestamp).Seconds()
	if elapsed <= 0 {
		return
	}
	prevIndex := make(map[string]QueueView, len(prev.Queues))
	for _, q := range prev.Queues {
		prevIndex[q.Name] = q
	}
	for i := range next.Queues {
		p, ok := prevIndex[next.Queues[i].Name]
		if !ok {
			continue
		}
		n := &next.Queues[i]
		n.Rates = Rates{
			AppendsPerSec:  rate(p.Counters["appends"], n.Counters["appends"], elapsed),
			ReadsPerSec:    rate(p.Counters["reads"], n.Counters["reads"], elapsed),
			EnqueuesPerSec: rate(p.Counters["enqueues"], n.Counters["enqueues"], elapsed),
			DequeuesPerSec: rate(p.Counters["dequeues"], n.Counters["dequeues"], elapsed),
			BytesInPerSec:  rate(p.Counters["bytes_in"], n.Counters["bytes_in"], elapsed),
			BytesOutPerSec: rate(p.Counters["bytes_out"], n.Counters["bytes_out"], elapsed),
		}
	}
}

func rate(prev, cur int64, elapsed float64) float64 {
	if cur <= prev {
		return 0
	}
	return float64(cur-prev) / elapsed
}

func totalize(snap Snapshot) TotalsView {
	t := TotalsView{Queues: len(snap.Queues)}
	for _, q := range snap.Queues {
		if v := q.Gauges["messages"]; v > 0 {
			t.Messages += uint64(v)
		}
		t.Bytes += q.Gauges["size_bytes"]
	}
	return t
}
