package metrics

import (
	"time"
)

var benchHist = HistogramView{
	Bounds: []float64{0.001, 0.01, 0.1}, Counts: []uint64{3, 2, 1, 1},
	Sum: 0.9, Total: 7, P50: 0.001, P90: 0.01, P95: 0.02, P99: 0.05, P999: 0.1, Max: 0.1,
}

func benchSnapshot() Snapshot {
	return Snapshot{
		Timestamp: time.Unix(0, 0).UTC(),
		Build:     BuildView{SpecVersion: 1, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Runtime: RuntimeView{
			Goroutines: 7, GOMAXPROCS: 4, UptimeSec: 12.5,
			GoroutineStates: GoroutineStates{Running: 1, Runnable: 2, Waiting: 4},
			Heap:            HeapView{Alloc: 1, Sys: 2, Inuse: 3, Idle: 4, Released: 5, Objects: 6, TotalAlloc: 7, TotalFree: 8},
			Stack:           MemoryClassView{Inuse: 1, Sys: 2},
			Metadata:        MemoryClassView{Inuse: 3, Sys: 4},
		},
		Process: ProcessView{CPUPercent: 1.25, CPUSecondsUser: 1.5, CPUSecondsSys: 0.5, RSSBytes: 4096, VSZBytes: 8192, Threads: 9, OpenFDs: 21},
		GC: GCView{Cycles: 3, PauseTotalMs: 4, NextGCBytes: 5, MemLimitBytes: 6,
			Pause: PauseView{P50Ms: 0.1, P90Ms: 0.2, P99Ms: 0.3, MaxMs: 0.4}},
		Scheduler: SchedulerView{Latency: LatencyView{P50: 0.1, P90: 0.2, P99: 0.3}},
		Locks: LocksView{GlobalWaitSec: 0.25, Observers: []LockView{{
			Name: "node.mu", Waits: 4, TotalWaitMs: 2, MaxWaitMs: 1.5, P50WaitMs: 0.1, P99WaitMs: 1.2,
		}}},
		Storage: StorageView{DataDir: "/data/mikyu", Queues: 2, Segments: 3, SizeBytes: 4096, CacheBytes: 1, CacheLimit: 2, CacheHits: 30, CacheMisses: 2, CommittedBytes: 1000, FlushedBytes: 900, PendingBytes: 100, OffsetStoreBytes: 7},
		Totals:  TotalsView{Queues: 2, Messages: 11, Bytes: 5120},
		Queues: []QueueView{
			{
				Name: "events", Kind: "log",
				Counters:   map[string]int64{"appends": 10, "reads": 4, "bytes_in": 2048, "bytes_out": 1024, "op_append": 3, "op_fetch": 2, "queue_faults": 1},
				Gauges:     map[string]int64{"messages": 10, "size_bytes": 2048, "segments": 1, "wal_pending": 2},
				Histograms: map[string]HistogramView{"append_duration": benchHist, "read_duration": benchHist},
				Rates:      Rates{AppendsPerSec: 1, ReadsPerSec: 2, BytesInPerSec: 3, BytesOutPerSec: 4},
				Producers:  ProducerView{InFlight: 8, BlockedOnWAL: 2, BlockEvents: 3},
				Consumers:  ConsumerView{InFlight: 4, ParkedFetch: 2, FetchParkEvents: 42},
				Backpressure: BackpressureView{
					WALChannelFill: 0.003, WALByteFill: 0.0006, QueueFullEvents: 1,
				},
				Groups: []GroupView{{Name: "analytics", Committed: 900, HighWater: 1000, Lag: 100}},
			},
			{
				Name:     "jobs",
				Kind:     "task",
				Counters: map[string]int64{"enqueues": 5, "dequeues": 4, "acks": 3},
				Gauges:   map[string]int64{"messages": 2, "size_bytes": 1024},
				Tasks:    TaskView{Active: 42, InFlight: 8, Scheduled: 34, DeadLetters: 2, CheckpointLag: 3},
			},
		},
	}
}
