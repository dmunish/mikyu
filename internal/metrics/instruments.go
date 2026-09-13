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
