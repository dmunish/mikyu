package bench

import (
	"cmp"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
)

// histogramBuckets is the resolution of the log-linear latency histogram.
const histogramBuckets = 10000

// tailCapacity bounds the retained tail samples.
const tailCapacity = 4096

// histogramRatio is the geometric step between adjacent bucket bounds.
var histogramRatio float64

// histogramBounds is the ascending edge of every histogram bucket; index i covers [bounds[i-1], bounds[i]).
var histogramBounds = func() []float64 {
	const minNs = 1.0
	const maxNs = float64(time.Hour)
	ratio := math.Pow(maxNs/minNs, 1.0/float64(histogramBuckets-1))
	histogramRatio = ratio
	bounds := make([]float64, histogramBuckets)
	v := minNs
	for i := range bounds {
		bounds[i] = v
		v *= ratio
	}
	return bounds
}()

// Histogram is a log-linear latency histogram; the zero value is ready to use and safe for concurrent calls.
type Histogram struct {
	once   sync.Once
	counts []atomic.Uint64
	sum    atomic.Uint64
	total  atomic.Uint64
}

// init allocates the bucket counters once, so the zero value works without a constructor.
func (h *Histogram) init() {
	h.once.Do(func() { h.counts = make([]atomic.Uint64, histogramBuckets+1) })
}

// Record adds one latency observation.
func (h *Histogram) Record(v time.Duration) {
	h.init()
	ns := float64(v.Nanoseconds())
	idx, _ := slices.BinarySearch(histogramBounds, ns)
	h.counts[idx].Add(1)
	h.sum.Add(uint64(v.Nanoseconds()))
	h.total.Add(1)
}

// Merge folds another histogram's observations into this one.
func (h *Histogram) Merge(other *Histogram) {
	if other == nil {
		return
	}
	h.init()
	other.init()
	for i := range h.counts {
		if n := other.counts[i].Load(); n > 0 {
			h.counts[i].Add(n)
		}
	}
	h.sum.Add(other.sum.Load())
	h.total.Add(other.total.Load())
}

// Percentile returns the latency at rank p, interpolated within its bucket.
func (h *Histogram) Percentile(p float64) time.Duration {
	h.init()
	counts := make([]uint64, len(h.counts))
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	return percentileFromCounts(counts, h.total.Load(), p)
}

// Max returns the upper edge of the highest non-empty bucket.
func (h *Histogram) Max() time.Duration {
	h.init()
	counts := make([]uint64, len(h.counts))
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	return maxLatency(counts)
}

// Len returns the number of observations recorded.
func (h *Histogram) Len() int64 { return int64(h.total.Load()) }

// view renders the histogram's summary percentiles.
func (h *Histogram) view() result.HistogramView {
	h.init()
	counts := make([]uint64, len(h.counts))
	var total uint64
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
		total += counts[i]
	}
	return viewFromCounts(counts, total)
}

// deltaView returns the observations added since the previous call and advances prev to the current counts.
func (h *Histogram) deltaView(prev *[]uint64) result.HistogramView {
	h.init()
	if *prev == nil {
		*prev = make([]uint64, len(h.counts))
	}
	delta := make([]uint64, len(h.counts))
	var total uint64
	for i := range h.counts {
		cur := h.counts[i].Load()
		delta[i] = cur - (*prev)[i]
		(*prev)[i] = cur
		total += delta[i]
	}
	return viewFromCounts(delta, total)
}

// viewFromCounts renders percentiles and a maximum from bucket counts.
func viewFromCounts(counts []uint64, total uint64) result.HistogramView {
	return result.HistogramView{
		P50:  percentileFromCounts(counts, total, 0.50).Nanoseconds(),
		P90:  percentileFromCounts(counts, total, 0.90).Nanoseconds(),
		P95:  percentileFromCounts(counts, total, 0.95).Nanoseconds(),
		P99:  percentileFromCounts(counts, total, 0.99).Nanoseconds(),
		P999: percentileFromCounts(counts, total, 0.999).Nanoseconds(),
		Max:  maxLatency(counts).Nanoseconds(),
	}
}

// percentileFromCounts interpolates the rank-p latency within its bucket.
func percentileFromCounts(counts []uint64, total uint64, p float64) time.Duration {
	if total == 0 {
		return 0
	}
	p = min(max(p, 0), 1)
	rank := max(uint64(p*float64(total)), 1)
	var seen uint64
	for i, n := range counts {
		if n == 0 {
			continue
		}
		if seen+n >= rank {
			lo, hi := boundAt(i-1), boundAt(i)
			frac := float64(rank-seen) / float64(n)
			return time.Duration(lo + frac*(hi-lo))
		}
		seen += n
	}
	return time.Duration(boundAt(len(counts) - 1))
}

// maxLatency returns the upper edge of the highest non-empty bucket.
func maxLatency(counts []uint64) time.Duration {
	for i, count := range slices.Backward(counts) {
		if count > 0 {
			return time.Duration(boundAt(i))
		}
	}
	return 0
}

// boundAt returns a bucket edge, clamping below zero and extending the overflow bucket.
func boundAt(i int) float64 {
	switch {
	case i < 0:
		return 0
	case i >= len(histogramBounds):
		return histogramBounds[len(histogramBounds)-1] * histogramRatio
	default:
		return histogramBounds[i]
	}
}

// Observation is the token returned by Start; it carries the scheduled and actual send times.
type Observation struct {
	scheduled time.Time
	started   time.Time
}

// Measure accumulates client-side latency for one run; it is safe for concurrent use.
type Measure struct {
	corrected   *Histogram
	uncorrected *Histogram
	tail        *tailSamples
	messages    atomic.Int64
	verified    atomic.Bool

	mu              sync.Mutex
	prevCorrected   []uint64
	prevUncorrected []uint64
	prevMessages    int64
}

// NewMeasure returns an empty measure.
func NewMeasure() *Measure {
	return &Measure{corrected: &Histogram{}, uncorrected: &Histogram{}, tail: &tailSamples{}}
}

// MarkVerified records that the run had verification enabled.
func (m *Measure) MarkVerified() { m.verified.Store(true) }

// Start records the intended send time and returns a token Finish completes.
func (m *Measure) Start(scheduled time.Time) Observation {
	return Observation{scheduled: scheduled, started: time.Now()}
}

// Finish records one open-loop observation: corrected uses the scheduled time, uncorrected the actual start.
func (m *Measure) Finish(obs Observation) {
	now := time.Now()
	corrected := now.Sub(obs.scheduled)
	m.corrected.Record(corrected)
	m.uncorrected.Record(now.Sub(obs.started))
	m.tail.record(corrected)
}

// Record records one closed-loop observation directly.
func (m *Measure) Record(latency time.Duration) {
	m.uncorrected.Record(latency)
	m.tail.record(latency)
}

// AddMessages counts how many queue messages an observation covered.
func (m *Measure) AddMessages(n int64) { m.messages.Add(n) }

// Snapshot returns the cumulative histograms and tail samples without resetting.
func (m *Measure) Snapshot() result.MetricsView {
	total := m.messages.Load()
	return result.MetricsView{
		LatencyCorrected:   m.corrected.view(),
		LatencyUncorrected: m.uncorrected.view(),
		TailSamplesNS:      m.tail.snapshot(total),
		Messages:           total,
		Verified:           m.verified.Load(),
	}
}

// SnapshotSince returns the observations recorded since the previous SnapshotSince call.
func (m *Measure) SnapshotSince(_ time.Time) result.MetricsView {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := m.messages.Load()
	delta := total - m.prevMessages
	m.prevMessages = total
	return result.MetricsView{
		LatencyCorrected:   m.corrected.deltaView(&m.prevCorrected),
		LatencyUncorrected: m.uncorrected.deltaView(&m.prevUncorrected),
		Messages:           delta,
		Verified:           m.verified.Load(),
	}
}

// tailSamples keeps the largest observations in a bounded min-heap.
type tailSamples struct {
	mu sync.Mutex
	h  tailHeap
}

// record keeps the newest observation if it belongs in the tail.
func (t *tailSamples) record(v time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.h.items) < tailCapacity {
		t.h.push(v)
		return
	}
	if v > t.h.min() {
		t.h.replaceMin(v)
	}
}

// snapshot returns up to the top 0.1% of observations, largest first.
func (t *tailSamples) snapshot(total int64) []int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.h.items) == 0 {
		return nil
	}
	k := min(max(int(total/1000), 1), len(t.h.items))
	vals := slices.Clone(t.h.items)
	slices.SortFunc(vals, func(a, b time.Duration) int { return cmp.Compare(b, a) })
	out := make([]int64, k)
	for i := range k {
		out[i] = vals[i].Nanoseconds()
	}
	return out
}

// tailHeap is a min-heap of durations kept small enough to hold the tail.
type tailHeap struct {
	items []time.Duration
}

// min returns the smallest retained duration; the caller must ensure the heap is non-empty.
func (h *tailHeap) min() time.Duration { return h.items[0] }

// push adds a duration and restores the heap property.
func (h *tailHeap) push(v time.Duration) {
	h.items = append(h.items, v)
	i := len(h.items) - 1
	for i > 0 {
		parent := (i - 1) / 2
		if h.items[parent] <= h.items[i] {
			break
		}
		h.items[parent], h.items[i] = h.items[i], h.items[parent]
		i = parent
	}
}

// replaceMin overwrites the smallest duration and restores the heap property.
func (h *tailHeap) replaceMin(v time.Duration) {
	h.items[0] = v
	h.siftDown(0)
}

// siftDown restores the heap property starting at i.
func (h *tailHeap) siftDown(i int) {
	n := len(h.items)
	for {
		smallest := i
		if l := 2*i + 1; l < n && h.items[l] < h.items[smallest] {
			smallest = l
		}
		if r := 2*i + 2; r < n && h.items[r] < h.items[smallest] {
			smallest = r
		}
		if smallest == i {
			return
		}
		h.items[i], h.items[smallest] = h.items[smallest], h.items[i]
		i = smallest
	}
}

// timerOverheadNS is the measured cost of one time.Now call, established at init.
var timerOverheadNS int64

// init measures the per-call time.Now cost, using enough calls to exceed a coarse clock's resolution.
func init() {
	const samples = 100_000
	start := time.Now()
	for range samples {
		_ = time.Now()
	}
	timerOverheadNS = int64(time.Since(start)) / samples
}

// TimerOverhead returns the package-init measured time.Now cost.
func TimerOverhead() time.Duration { return time.Duration(timerOverheadNS) }
