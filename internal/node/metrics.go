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

// newCollector builds a collector over a single runtime source, disabled when metrics are off.
func newCollector(mc MetricsConfig) *metrics.Collector {
	reg := metrics.NewRegistry(metrics.Config{Histograms: mc.Histograms})
	return metrics.New(reg, metrics.Config{
		Interval:   mc.Interval,
		History:    mc.History,
		Histograms: mc.Histograms,
		Disabled:   !mc.Enabled,
	}, NewRuntimeSource())
}

// Collector returns the node's metrics collector.
func (n *Node) Collector() *metrics.Collector { return n.metrics }

// Metrics returns the node's current live snapshot.
func (n *Node) Metrics(ctx context.Context) (metrics.Snapshot, error) {
	return n.metrics.Snapshot(ctx)
}
