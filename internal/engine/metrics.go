package engine

import (
	"context"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// Gauge names the engine publishes for every live queue.
const (
	GaugeRecords   = "messages"
	GaugeSegments   = "segments"
	GaugeSizeBytes  = "size_bytes"
	GaugeLastOffset = "last_offset"
	GaugeMinOffset  = "min_offset"
	GaugeKeyIndex   = "key_index"
)

// ioDurationBounds are latency buckets, in seconds, for storage operations.
var ioDurationBounds = []float64{
	1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1,
}

// queueMetrics caches a queue's instruments so hot paths avoid map lookups.
type queueMetrics struct {
	appends      *metrics.Counter
	reads        *metrics.Counter
	bytesIn      *metrics.Counter
	bytesOut     *metrics.Counter
	segmentRolls *metrics.Counter
	groupCommits *metrics.Counter
	appendDur    *metrics.Histogram
	readDur      *metrics.Histogram
	keyIndexSize *metrics.Gauge
}

func newQueueMetrics(scope *metrics.Scope) *queueMetrics {
	return &queueMetrics{
		appends:      scope.Counter("appends"),
		reads:        scope.Counter("reads"),
		bytesIn:      scope.Counter("bytes_in"),
		bytesOut:     scope.Counter("bytes_out"),
		segmentRolls: scope.Counter("segment_rolls"),
		groupCommits: scope.Counter("group_commits"),
		appendDur:    scope.Histogram("append_duration", ioDurationBounds),
		readDur:      scope.Histogram("read_duration", ioDurationBounds),
		keyIndexSize: scope.Gauge("key_index"),
	}
}

// walMetrics caches the instruments the WAL updates off the queue's hot path.
type walMetrics struct {
	flushes  *metrics.Counter
	flushDur *metrics.Histogram
	pending  *metrics.Gauge
}

func newWALMetrics(scope *metrics.Scope) *walMetrics {
	return &walMetrics{
		flushes:  scope.Counter("wal_flushes"),
		flushDur: scope.Histogram("wal_flush_duration", ioDurationBounds),
		pending:  scope.Gauge("wal_pending"),
	}
}

// scopeOf returns the single optional scope, or nil.
func scopeOf(scopes []*metrics.Scope) *metrics.Scope {
	if len(scopes) == 0 {
		return nil
	}
	return scopes[0]
}

// Collect walks every live queue and fills the pull-based gauges.
func (m *Manager) Collect(_ context.Context, dst *metrics.Snapshot) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, q := range m.queues {
		q.mu.RLock()
		qv := metrics.QueueView{
			Name: q.name,
			Kind: "internal",
			Gauges: map[string]int64{
				GaugeRecords:   int64(q.publishedOffset),
				GaugeSegments:  int64(len(q.segments)),
				GaugeSizeBytes: q.onDiskSizeLocked(),
				GaugeLastOffset: int64(q.nextOffset),
				GaugeMinOffset:  int64(q.minOffset),
				GaugeKeyIndex:   int64(len(q.keyIndex)),
			},
		}
		q.mu.RUnlock()
		dst.Queues = append(dst.Queues, qv)
	}
}

// onDiskSizeLocked sums the written bytes of every open segment. The caller
// must hold q.mu.
func (q *Queue) onDiskSizeLocked() int64 {
	var total int64
	for _, seg := range q.segments {
		total += seg.pos
	}
	return total
}

// recordAppend updates the append counters and latency histogram.
func (q *Queue) recordAppend(batch int, bytes int64, start time.Time) {
	q.m.appends.Add(int64(batch))
	q.m.bytesIn.Add(bytes)
	q.m.appendDur.Observe(time.Since(start).Seconds())
}

// recordRead updates the read counters and latency histogram.
func (q *Queue) recordRead(batch int, bytes int64, start time.Time) {
	q.m.reads.Add(int64(batch))
	q.m.bytesOut.Add(bytes)
	q.m.readDur.Observe(time.Since(start).Seconds())
}
