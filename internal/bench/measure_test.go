package bench

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHistogramAccuracy(t *testing.T) {
	const (
		samples = 1_000_000
		minNs   = 1_000.0
		maxNs   = 1e9
	)
	h := &Histogram{}
	rng := rand.New(rand.NewPCG(1, 2))
	for range samples {
		v := minNs * math.Pow(maxNs/minNs, rng.Float64())
		h.Record(time.Duration(v))
	}

	p50 := minNs * math.Sqrt(maxNs/minNs)
	p99 := minNs * math.Pow(maxNs/minNs, 0.99)
	assert.InDelta(t, p50, float64(h.Percentile(0.50)), 0.05*p50, "p50")
	assert.InDelta(t, p99, float64(h.Percentile(0.99)), 0.05*p99, "p99")
	assert.Equal(t, int64(samples), h.Len())
}

func TestCOCorrection(t *testing.T) {
	m := NewMeasure()
	past := time.Now().Add(-time.Second)
	for range 1000 {
		obs := m.Start(past)
		m.Finish(obs)
	}
	view := m.Snapshot()
	assert.Greater(t, view.LatencyCorrected.P99, view.LatencyUncorrected.P99, "a scheduled backlog must inflate corrected latency")
}

func TestConcurrentRecord(t *testing.T) {
	m := NewMeasure()
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 1000 {
				m.Record(time.Duration(i+1) * time.Microsecond)
				m.AddMessages(1)
			}
		}()
	}
	wg.Wait()

	view := m.Snapshot()
	assert.Equal(t, int64(100_000), view.Messages)
	assert.Positive(t, view.LatencyUncorrected.P50)
	assert.NotEmpty(t, view.TailSamplesNS)
}

func TestHistogramMerge(t *testing.T) {
	a, b := &Histogram{}, &Histogram{}
	for i := range 100 {
		a.Record(time.Duration(i+1) * time.Microsecond)
		b.Record(time.Duration(i+1) * time.Millisecond)
	}
	a.Merge(b)
	assert.Equal(t, int64(200), a.Len())
	assert.Greater(t, a.Max(), time.Millisecond)
}

func TestSnapshotSinceDeltas(t *testing.T) {
	m := NewMeasure()
	m.Record(10 * time.Microsecond)
	m.AddMessages(1)
	first := m.SnapshotSince(time.Now())
	assert.Equal(t, int64(1), first.Messages)
	second := m.SnapshotSince(time.Now())
	assert.Zero(t, second.Messages)
	m.Record(20 * time.Microsecond)
	m.AddMessages(3)
	third := m.SnapshotSince(time.Now())
	assert.Equal(t, int64(3), third.Messages)
}

func TestTimerOverhead(t *testing.T) {
	assert.Positive(t, TimerOverhead())
	assert.Less(t, TimerOverhead(), time.Microsecond)
}
