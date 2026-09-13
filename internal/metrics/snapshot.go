package metrics

import (
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// Snapshot is the immutable state of a Mikyu node at one instant.
type Snapshot struct {
	Disabled  bool        `json:"disabled,omitempty"`
	Timestamp time.Time   `json:"timestamp"`
	Runtime   RuntimeView `json:"runtime"`
	Process   ProcessView `json:"process"`
	Storage   StorageView `json:"storage"`
	Queues    []QueueView `json:"queues"`
	Totals    TotalsView  `json:"totals"`
}

// RuntimeView reports Go runtime gauges.
type RuntimeView struct {
	Goroutines int     `json:"goroutines"`
	HeapAlloc  int64   `json:"heap_alloc"`
	HeapInuse  int64   `json:"heap_inuse"`
	GCCount    int64   `json:"gc_count"`
	UptimeSec  float64 `json:"uptime_seconds"`
}

// ProcessView reports process-level resource gauges.
type ProcessView struct {
	CPUPercent float64 `json:"cpu_percent"`
	RSSBytes   int64   `json:"rss_bytes"`
}

// StorageView reports aggregate on-disk storage gauges.
type StorageView struct {
	DataDir   string `json:"data_dir,omitempty"`
	Segments  int64  `json:"segments"`
	SizeBytes int64  `json:"size_bytes"`
}

// QueueView is a queue's spec and instrument readings.
type QueueView struct {
	Name       string                       `json:"name"`
	Kind       string                       `json:"kind"`
	Spec       spec.Spec                    `json:"spec,omitzero"`
	Effective  spec.Spec                    `json:"effective,omitzero"`
	Counters   map[string]int64             `json:"counters,omitempty"`
	Gauges     map[string]int64             `json:"gauges,omitempty"`
	Histograms map[string]HistogramSnapshot `json:"histograms,omitempty"`
	Groups     []GroupView                  `json:"groups,omitempty"`
	Rates      Rates                        `json:"rates,omitzero"`
}

// GroupView is a consumer group's committed position.
type GroupView struct {
	Name      string `json:"name"`
	Committed uint64 `json:"committed"`
	HighWater uint64 `json:"high_water"`
	Lag       uint64 `json:"lag"`
}

// Rates holds per-queue derivatives computed by the collector.
type Rates struct {
	AppendsPerSec  float64 `json:"appends_per_sec"`
	ReadsPerSec    float64 `json:"reads_per_sec"`
	EnqueuesPerSec float64 `json:"enqueues_per_sec"`
	DequeuesPerSec float64 `json:"dequeues_per_sec"`
	BytesInPerSec  float64 `json:"bytes_in_per_sec"`
	BytesOutPerSec float64 `json:"bytes_out_per_sec"`
}

// TotalsView aggregates every queue.
type TotalsView struct {
	Queues   int    `json:"queues"`
	Messages uint64 `json:"messages"`
	Bytes    int64  `json:"bytes"`
}

// IterQueues yields each queue view in name order.
func (s Snapshot) IterQueues() iter.Seq[QueueView] {
	queues := slices.Clone(s.Queues)
	slices.SortFunc(queues, func(a, b QueueView) int {
		return strings.Compare(a.Name, b.Name)
	})
	return slices.Values(queues)
}

// Queue returns the named queue view.
func (s Snapshot) Queue(name string) (QueueView, bool) {
	for _, q := range s.Queues {
		if q.Name == name {
			return q, true
		}
	}
	return QueueView{}, false
}
