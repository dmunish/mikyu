package metrics

import (
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
