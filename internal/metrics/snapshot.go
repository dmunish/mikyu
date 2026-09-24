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
	Disabled  bool          `json:"disabled,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
	Build     BuildView     `json:"build"`
	Runtime   RuntimeView   `json:"runtime"`
	Process   ProcessView   `json:"process"`
	GC        GCView        `json:"gc"`
	Scheduler SchedulerView `json:"scheduler"`
	Locks     LocksView     `json:"locks"`
	Session   spec.Spec     `json:"session,omitzero"`
	Storage   StorageView   `json:"storage"`
	Queues    []QueueView   `json:"queues"`
	Totals    TotalsView    `json:"totals"`
}

// BuildView identifies the binary that produced the snapshot.
type BuildView struct {
	SpecVersion uint16 `json:"spec_version"`
	GoVersion   string `json:"go_version"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
}

// RuntimeView reports Go runtime gauges and memory classes.
type RuntimeView struct {
	Goroutines      int             `json:"goroutines"`
	GOMAXPROCS      int             `json:"gomaxprocs"`
	GoroutineStates GoroutineStates `json:"goroutine_states"`
	Heap            HeapView        `json:"heap"`
	Stack           MemoryClassView `json:"stack"`
	Metadata        MemoryClassView `json:"metadata"`
	UptimeSec       float64         `json:"uptime_seconds"`
}

// GoroutineStates counts goroutines by scheduling state.
type GoroutineStates struct {
	Running  int64 `json:"running"`
	Runnable int64 `json:"runnable"`
	Waiting  int64 `json:"waiting"`
}

// HeapView reports Go heap accounting in bytes.
type HeapView struct {
	Alloc      int64 `json:"alloc"`
	Sys        int64 `json:"sys"`
	Inuse      int64 `json:"inuse"`
	Idle       int64 `json:"idle"`
	Released   int64 `json:"released"`
	Objects    int64 `json:"objects"`
	TotalAlloc int64 `json:"total_alloc"`
	TotalFree  int64 `json:"total_free"`
}

// MemoryClassView reports one runtime memory class in bytes.
type MemoryClassView struct {
	Inuse int64 `json:"inuse"`
	Sys   int64 `json:"sys"`
}

// ProcessView reports process-level resource gauges.
type ProcessView struct {
	CPUPercent     float64 `json:"cpu_percent"`
	CPUSecondsUser float64 `json:"cpu_seconds_user"`
	CPUSecondsSys  float64 `json:"cpu_seconds_sys"`
	RSSBytes       int64   `json:"rss_bytes"`
	VSZBytes       int64   `json:"vsz_bytes"`
	Threads        int64   `json:"threads"`
	OpenFDs        int64   `json:"open_fds"`
}

// GCView reports garbage-collector pacing and pause distributions.
type GCView struct {
	Cycles        int64     `json:"cycles"`
	PauseTotalMs  float64   `json:"pause_total_ms"`
	NextGCBytes   int64     `json:"next_gc_bytes"`
	MemLimitBytes int64     `json:"mem_limit_bytes"`
	Pause         PauseView `json:"pause"`
}

// PauseView reports GC stop-the-world pause percentiles in milliseconds.
type PauseView struct {
	P50Ms float64 `json:"p50_ms"`
	P90Ms float64 `json:"p90_ms"`
	P99Ms float64 `json:"p99_ms"`
	MaxMs float64 `json:"max_ms"`
}

// SchedulerView reports Go scheduler latency distributions.
type SchedulerView struct {
	Latency LatencyView `json:"latency"`
}

// LatencyView reports percentile latencies in seconds.
type LatencyView struct {
	P50 float64 `json:"p50"`
	P90 float64 `json:"p90"`
	P99 float64 `json:"p99"`
}

// LocksView reports lock contention, globally and per observer.
type LocksView struct {
	GlobalWaitSec float64    `json:"global_wait_seconds"`
	Observers     []LockView `json:"observers,omitempty"`
}

// LockView reports one observed lock's contention history.
type LockView struct {
	Name        string  `json:"name"`
	Waits       int64   `json:"waits"`
	TotalWaitMs float64 `json:"total_wait_ms"`
	MaxWaitMs   float64 `json:"max_wait_ms"`
	P50WaitMs   float64 `json:"p50_wait_ms"`
	P99WaitMs   float64 `json:"p99_wait_ms"`
	CurrentHeld int64   `json:"current_held"`
}

// StorageView reports aggregate on-disk and cache storage gauges.
type StorageView struct {
	DataDir          string `json:"data_dir,omitempty"`
	Queues           int    `json:"queues"`
	Segments         int64  `json:"segments"`
	SizeBytes        int64  `json:"size_bytes"`
	CacheBytes       int64  `json:"cache_bytes"`
	CacheLimit       int64  `json:"cache_limit"`
	CacheHits        int64  `json:"cache_hits"`
	CacheMisses      int64  `json:"cache_misses"`
	CommittedBytes   int64  `json:"committed_bytes"`
	FlushedBytes     int64  `json:"flushed_bytes"`
	PendingBytes     int64  `json:"pending_bytes"`
	OffsetStoreBytes int64  `json:"offset_store_bytes"`
}

// QueueView is a queue's spec, instrument readings, and derived views.
type QueueView struct {
	Name       string                   `json:"name"`
	Kind       string                   `json:"kind"`
	Spec       spec.Spec                `json:"spec,omitzero"`
	Derived    spec.Spec                `json:"derived,omitzero"`
	Effective  spec.Spec                `json:"effective,omitzero"`
	Counters   map[string]int64         `json:"counters,omitempty"`
	Gauges     map[string]int64         `json:"gauges,omitempty"`
	Rates      Rates                    `json:"rates,omitzero"`
	Histograms map[string]HistogramView `json:"histograms,omitempty"`

	Producers    ProducerView     `json:"producers"`
	Consumers    ConsumerView     `json:"consumers"`
	Backpressure BackpressureView `json:"backpressure"`
	Tasks        TaskView         `json:"tasks,omitzero"`
	Groups       []GroupView      `json:"groups,omitempty"`
}

// HistogramView is a histogram snapshot with derived quantiles.
type HistogramView struct {
	Bounds []float64 `json:"bounds"`
	Counts []uint64  `json:"counts"`
	Sum    float64   `json:"sum"`
	Total  uint64    `json:"total"`
	P50    float64   `json:"p50"`
	P90    float64   `json:"p90"`
	P95    float64   `json:"p95"`
	P99    float64   `json:"p99"`
	P999   float64   `json:"p999"`
	Max    float64   `json:"max"`
}

// ProducerView reports producer concurrency and WAL backpressure.
type ProducerView struct {
	InFlight     int64 `json:"in_flight"`
	BlockedOnWAL int64 `json:"blocked_on_wal"`
	BlockEvents  int64 `json:"block_events"`
}

// ConsumerView reports consumer concurrency and parked long-polls.
type ConsumerView struct {
	InFlight          int64 `json:"in_flight"`
	ParkedFetch       int64 `json:"parked_fetch"`
	ParkedDequeue     int64 `json:"parked_dequeue"`
	FetchParkEvents   int64 `json:"fetch_park_events"`
	DequeueParkEvents int64 `json:"dequeue_park_events"`
}

// BackpressureView reports flow-control signals and their event counts.
type BackpressureView struct {
	WALChannelFill   float64 `json:"wal_channel_fill"`
	WALByteFill      float64 `json:"wal_byte_fill"`
	QueueFullEvents  int64   `json:"queue_full_events"`
	ProducerBlocks   int64   `json:"producer_blocks"`
	ConsumerParks    int64   `json:"consumer_parks"`
	RetryBudgetDrops int64   `json:"retry_budget_drops"`
}

// TaskView reports task-queue occupancy and checkpoint health.
type TaskView struct {
	Active         int64 `json:"active"`
	InFlight       int64 `json:"in_flight"`
	Scheduled      int64 `json:"scheduled"`
	DeadLetters    int64 `json:"dead_letters"`
	Uncheckpointed int64 `json:"uncheckpointed"`
	ScheduleHeap   int64 `json:"schedule_heap"`
	CheckpointLSN  int64 `json:"checkpoint_lsn"`
	Dirty          int64 `json:"dirty"`
	CheckpointLag  int64 `json:"checkpoint_lag"`
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