package mikyu

import "github.com/dmunish/mikyu/internal/metrics"

// Metrics is a snapshot of live telemetry: what the node and its
// queues were doing at one instant.
//
// The snapshot carries build information, runtime and process gauges
// (goroutines, heap, CPU, file descriptors), garbage collector and
// scheduler readings, lock contention, per-queue counters, gauges,
// rates and histograms, and storage totals. Queue views know their kind
// and carry the fields that kind owns: a task queue reports its
// in-flight and dead-letter counts, a log queue reports its segments
// and consumer groups.
//
// Read one snapshot through Client.Metrics, or a stream of them
// through Client.SubscribeSnapshots. History is available on a daemon's
// metrics HTTP endpoint.
type Metrics = metrics.Snapshot
