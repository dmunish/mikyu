package metrics

import (
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounterConcurrentInc(t *testing.T) {
	var c Counter
	const goroutines, each = 100, 1000
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range each {
				c.Inc()
			}
		}()
	}
	wg.Wait()
	assert.EqualValues(t, goroutines*each, c.Load())
}

func TestGaugeSetAndAdd(t *testing.T) {
	var g Gauge
	g.Set(5)
	g.Add(3)
	assert.EqualValues(t, 8, g.Load())
	g.Add(-10)
	assert.EqualValues(t, -2, g.Load())
}

func TestHistogramBuckets(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	for _, v := range []float64{0.5, 1, 1.5, 2, 2.5, 3, 4} {
		h.Observe(v)
	}
	snap := h.Snapshot()
	require.Equal(t, []float64{1, 2, 3}, snap.Bounds)
	require.Equal(t, []uint64{2, 2, 2, 1}, snap.Counts)
	assert.EqualValues(t, 7, snap.Total)
	assert.InDelta(t, 14.5, snap.Sum, 1e-9)
}

func TestHistogramConcurrentObserve(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	var wg sync.WaitGroup
	wg.Add(8)
	for range 8 {
		go func() {
			defer wg.Done()
			for range 1000 {
				h.Observe(1.5)
			}
		}()
	}
	wg.Wait()
	snap := h.Snapshot()
	assert.EqualValues(t, 8000, snap.Total)
	assert.InDelta(t, 12000, snap.Sum, 1e-6)
	assert.EqualValues(t, 8000, snap.Counts[1])
}

func TestHistogramPercentileEmpty(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	snap := h.Snapshot()
	assert.Zero(t, snap.Percentile(0.5))
	assert.Zero(t, snap.Percentile(0.99))
	assert.Zero(t, snap.Max())
}

func TestHistogramPercentileSingleValue(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	h.Observe(1.5)
	snap := h.Snapshot()
	assert.InDelta(t, 1.5, snap.Percentile(0.5), 1e-9)
}

func TestHistogramPercentileAllInFirstBucket(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	for range 100 {
		h.Observe(0.5)
	}
	snap := h.Snapshot()
	for _, p := range []float64{0.1, 0.5, 0.9, 1.0} {
		got := snap.Percentile(p)
		assert.GreaterOrEqual(t, got, 0.0)
		assert.LessOrEqual(t, got, 1.0)
	}
}

func TestHistogramPercentileAllInLastBucket(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	for range 100 {
		h.Observe(4)
	}
	snap := h.Snapshot()
	got := snap.Percentile(0.99)
	assert.False(t, math.IsNaN(got))
	assert.LessOrEqual(t, got, 3.0)
}

func TestHistogramPercentileAtBoundary(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	h.Observe(2)
	snap := h.Snapshot()
	require.EqualValues(t, []uint64{0, 1, 0, 0}, snap.Counts)
	assert.InDelta(t, 1.5, snap.Percentile(0.5), 1e-9)
}

func TestHistogramPercentileMonotonic(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3, 4, 5})
	for _, v := range []float64{0.2, 0.9, 1.1, 2.5, 2.9, 3.5, 4.4, 4.9, 5.5, 7.0} {
		h.Observe(v)
	}
	snap := h.Snapshot()
	p50 := snap.Percentile(0.50)
	p90 := snap.Percentile(0.90)
	p99 := snap.Percentile(0.99)
	p999 := snap.Percentile(0.999)
	max := snap.Max()
	assert.LessOrEqual(t, p50, p90+1e-9)
	assert.LessOrEqual(t, p90, p99+1e-9)
	assert.LessOrEqual(t, p99, p999+1e-9)
	assert.LessOrEqual(t, p999, max+1e-9)
}

func TestHistogramPercentileKnownDistribution(t *testing.T) {
	bounds := []float64{10, 20, 30, 40, 50, 60, 70, 80, 90, 100}
	h := NewHistogram(bounds)
	for i := range 100 {
		h.Observe(float64(i) + 0.5)
	}
	snap := h.Snapshot()
	assert.InDelta(t, 50, snap.Percentile(0.50), 5)
	assert.InDelta(t, 99, snap.Percentile(0.99), 5)
}

func TestHistogramMaxFromCounts(t *testing.T) {
	h := NewHistogram([]float64{1, 2, 3})
	for _, v := range []float64{0.5, 1.5, 2.5} {
		h.Observe(v)
	}
	snap := h.Snapshot()
	assert.InDelta(t, 3, snap.Max(), 1e-9)
	assert.Zero(t, (&Histogram{}).Snapshot().Max())
}
