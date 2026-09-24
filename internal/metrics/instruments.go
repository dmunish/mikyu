package metrics

import (
	"math"
	"slices"
	"sync/atomic"
)

// Counter is a monotonically increasing integer instrument.
type Counter struct{ v atomic.Int64 }

// Add increases the counter by n.
func (c *Counter) Add(n int64) {
	if c == nil {
		return
	}
	c.v.Add(n)
}

// Inc increases the counter by one.
func (c *Counter) Inc() { c.Add(1) }

// Load reads the current counter value.
func (c *Counter) Load() int64 {
	if c == nil {
		return 0
	}
	return c.v.Load()
}

// Gauge is an integer instrument that may move in either direction.
type Gauge struct{ v atomic.Int64 }

// Set stores n.
func (g *Gauge) Set(n int64) {
	if g == nil {
		return
	}
	g.v.Store(n)
}

// Add increases the gauge by n.
func (g *Gauge) Add(n int64) {
	if g == nil {
		return
	}
	g.v.Add(n)
}

// Load reads the current gauge value.
func (g *Gauge) Load() int64 {
	if g == nil {
		return 0
	}
	return g.v.Load()
}

// HistogramSnapshot is an immutable point-in-time copy of a histogram.
type HistogramSnapshot struct {
	Bounds []float64 `json:"bounds"`
	Counts []uint64  `json:"counts"`
	Sum    float64   `json:"sum"`
	Total  uint64    `json:"total"`
}

// Percentile returns the approximate p-th quantile, interpolating within the rank bucket.
func (h HistogramSnapshot) Percentile(p float64) float64 {
	if h.Total == 0 || len(h.Counts) == 0 {
		return 0
	}
	p = min(max(p, 0), 1)
	rank := p * float64(h.Total)
	var cumulative uint64
	for i, count := range h.Counts {
		prev := cumulative
		cumulative += count
		if float64(cumulative) < rank {
			continue
		}
		lower, upper := h.interval(i)
		if count == 0 || upper <= lower {
			return upper
		}
		frac := (rank - float64(prev)) / float64(count)
		return lower + frac*(upper-lower)
	}
	if len(h.Bounds) > 0 {
		return h.Bounds[len(h.Bounds)-1]
	}
	return 0
}

// interval returns the lower and upper bounds of bucket i.
func (h HistogramSnapshot) interval(i int) (lower, upper float64) {
	if i > 0 && i-1 < len(h.Bounds) {
		lower = h.Bounds[i-1]
	}
	upper = lower
	switch {
	case i < len(h.Bounds):
		upper = h.Bounds[i]
	case len(h.Bounds) > 0:
		upper = h.Bounds[len(h.Bounds)-1]
	}
	return lower, upper
}

// Max returns the highest bound with a non-zero count, or zero when empty.
func (h HistogramSnapshot) Max() float64 {
	for i := len(h.Counts) - 1; i >= 0; i-- {
		if h.Counts[i] == 0 {
			continue
		}
		if i < len(h.Bounds) {
			return h.Bounds[i]
		}
		if len(h.Bounds) > 0 {
			return h.Bounds[len(h.Bounds)-1]
		}
		return 0
	}
	return 0
}

// viewOf renders a histogram snapshot as its wire view.
func viewOf(s HistogramSnapshot) HistogramView {
	return HistogramView{
		Bounds: s.Bounds,
		Counts: s.Counts,
		Sum:    s.Sum,
		Total:  s.Total,
		P50:    s.Percentile(0.50),
		P90:    s.Percentile(0.90),
		P95:    s.Percentile(0.95),
		P99:    s.Percentile(0.99),
		P999:   s.Percentile(0.999),
		Max:    s.Max(),
	}
}

// Histogram records the distribution of float64 observations into fixed buckets.
type Histogram struct {
	bounds []float64
	counts []atomic.Uint64
	sum    atomic.Uint64
	total  atomic.Uint64
}

// NewHistogram builds a histogram with the given upper bounds.
func NewHistogram(bounds []float64) *Histogram {
	b := slices.Clone(bounds)
	slices.Sort(b)
	return &Histogram{bounds: b, counts: make([]atomic.Uint64, len(b)+1)}
}

// Observe records one observation.
func (h *Histogram) Observe(v float64) {
	if h == nil || len(h.counts) == 0 {
		return
	}
	i, _ := slices.BinarySearch(h.bounds, v)
	h.counts[i].Add(1)
	h.total.Add(1)
	for {
		old := h.sum.Load()
		next := math.Float64bits(math.Float64frombits(old) + v)
		if h.sum.CompareAndSwap(old, next) {
			return
		}
	}
}

// Snapshot copies the histogram's current state.
func (h *Histogram) Snapshot() HistogramSnapshot {
	if h == nil || len(h.counts) == 0 {
		return HistogramSnapshot{}
	}
	s := HistogramSnapshot{
		Bounds: slices.Clone(h.bounds),
		Counts: make([]uint64, len(h.counts)),
		Sum:    math.Float64frombits(h.sum.Load()),
		Total:  h.total.Load(),
	}
	for i := range h.counts {
		s.Counts[i] = h.counts[i].Load()
	}
	return s
}
