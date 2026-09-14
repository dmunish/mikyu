package node

import (
	"context"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
)

// MetricsConfig carries the session-level settings that shape the node's collector.
type MetricsConfig struct {
	Enabled    bool
	Interval   time.Duration
	History    int
	Histograms bool
}

// AcquireOption configures how a freshly opened node builds its collector.
type AcquireOption func(*MetricsConfig)

// WithMetrics overrides the collector settings for a newly opened node.
func WithMetrics(cfg MetricsConfig) AcquireOption {
	return func(dst *MetricsConfig) { *dst = cfg }
}

// MetricsFromSession extracts the collector settings from a resolved session.
func MetricsFromSession(s spec.Session) MetricsConfig {
	return MetricsConfig{
		Enabled:    s.MetricsEnabled,
		Interval:   s.MetricsInterval.Duration(),
		History:    s.MetricsHistory,
		Histograms: s.MetricsHistograms,
	}
}

// preregisterServerMetrics ensures the server scope instruments are created
// before the collector tick starts, eliminating a race between scope creation
// and concurrent reads in the tick loop.
func preregisterServerMetrics(reg *metrics.Registry) {
	scope := reg.Scope("server")
	_ = scope.Counter("op_create_queue")
	_ = scope.Counter("op_delete_queue")
	_ = scope.Counter("op_list_queues")
	_ = scope.Counter("op_update_queue")
	_ = scope.Counter("op_describe_queue")
	_ = scope.Counter("op_truncate")
	_ = scope.Counter("op_info")
	_ = scope.Counter("op_append")
	_ = scope.Counter("op_append_batch")
	_ = scope.Counter("op_read")
	_ = scope.Counter("op_read_batch")
	_ = scope.Counter("op_last_offset")
	_ = scope.Counter("op_commit")
	_ = scope.Counter("op_committed")
	_ = scope.Counter("op_fetch")
	_ = scope.Counter("op_enqueue")
	_ = scope.Counter("op_dequeue")
	_ = scope.Counter("op_ack")
	_ = scope.Counter("op_nack")
	_ = scope.Counter("op_extend")
	_ = scope.Counter("op_dead_letters")
	_ = scope.Counter("op_dead_letter_ack")
	_ = scope.Counter("op_dead_letter_ack")
	_ = scope.Histogram("op_create_queue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_delete_queue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_list_queues_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_update_queue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_describe_queue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_truncate_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_info_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_append_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_append_batch_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_read_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_read_batch_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_last_offset_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_commit_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_committed_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_fetch_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_enqueue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_dequeue_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_ack_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_nack_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_extend_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_dead_letters_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Histogram("op_dead_letter_ack_duration", []float64{1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1})
	_ = scope.Gauge("connections")
}

// newRegistry builds the node's instrument registry from its session settings.
func newRegistry(mc MetricsConfig) *metrics.Registry {
	return metrics.NewRegistry(metrics.Config{Histograms: mc.Histograms})
}

// newCollector builds a collector over reg and the given sources, disabled when
// metrics are off.
func newCollector(reg *metrics.Registry, mc MetricsConfig, sources ...metrics.Source) *metrics.Collector {
	return metrics.New(reg, metrics.Config{
		Interval:   mc.Interval,
		History:    mc.History,
		Histograms: mc.Histograms,
		Disabled:   !mc.Enabled,
	}, sources...)
}

// Collector returns the node's metrics collector.
func (n *Node) Collector() *metrics.Collector { return n.metrics }

// Metrics returns the node's current live snapshot.
func (n *Node) Metrics(ctx context.Context) (metrics.Snapshot, error) {
	return n.metrics.Snapshot(ctx)
}
