package metrics

import (
	"io"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

// BenchmarkSnapshotCollectorScrape measures one full /metrics scrape as
// promhttp serves it: registry gather plus exposition rendering to the wire.
func BenchmarkSnapshotCollectorScrape(b *testing.B) {
	snap := benchSnapshot()
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(
		func() (Snapshot, error) { return snap, nil },
		CollectOptions{PerQueue: true, Histograms: true},
	))
	for b.Loop() {
		families, err := reg.Gather()
		if err != nil {
			b.Fatal(err)
		}
		for _, mf := range families {
			if _, err := expfmt.MetricFamilyToText(io.Discard, mf); err != nil {
				b.Fatal(err)
			}
		}
	}
}

// BenchmarkSnapshotCollectorGather isolates the gather half of a scrape, the
// part that walks the snapshot and builds dto metrics.
func BenchmarkSnapshotCollectorGather(b *testing.B) {
	snap := benchSnapshot()
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(
		func() (Snapshot, error) { return snap, nil },
		CollectOptions{PerQueue: true, Histograms: true},
	))
	for b.Loop() {
		if _, err := reg.Gather(); err != nil {
			b.Fatal(err)
		}
	}
}
