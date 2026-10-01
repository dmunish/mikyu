package result

import (
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// Result is the result.json document: what ran, against what, and what it measured.
type Result struct {
	Version      int             `json:"version"`
	RunID        string          `json:"run_id"`
	Mode         string          `json:"mode"`
	Access       string          `json:"access"`
	AccessTarget string          `json:"access_target,omitempty"`
	StartedAt    time.Time       `json:"started_at"`
	EndedAt      time.Time       `json:"ended_at,omitzero"`
	Complete     bool            `json:"complete"`
	Aborted      string          `json:"aborted,omitempty"`
	Spec         spec.Spec       `json:"spec"`
	Workload     WorkloadView    `json:"workload"`
	Environment  EnvironmentView `json:"environment"`
	Phases       []PhaseView     `json:"phases"`
	Metrics      *MetricsView    `json:"metrics,omitempty"`
	Notes        []string        `json:"notes,omitempty"`
}

// WorkloadView is the resolved workload as the envelope records it.
type WorkloadView struct {
	Kind         string      `json:"kind"`
	Queues       int         `json:"queues"`
	MsgSize      int         `json:"msg_size"`
	Bytes        int64       `json:"bytes,omitempty"`
	Duration     string      `json:"duration,omitempty"`
	Rate         uint64      `json:"rate,omitempty"`
	RateSchedule []RatePoint `json:"rate_schedule,omitempty"`
	Workers      int         `json:"workers"`
	Batch        int         `json:"batch"`
	Keys         int         `json:"keys,omitempty"`
	Preload      int64       `json:"preload,omitempty"`
	Warmup       string      `json:"warmup"`
	AckRate      float64     `json:"ack_rate,omitempty"`
	CrashAfter   int64       `json:"crash_after_bytes,omitempty"`
	Seed         uint32      `json:"seed"`
}

// RatePoint is one step of a time-varying offered rate.
type RatePoint struct {
	At   string `json:"at"`
	Rate uint64 `json:"rate"`
}

// EnvironmentView records the host and build the numbers were produced on.
type EnvironmentView struct {
	Host            string `json:"host"`
	OS              string `json:"os"`
	Arch            string `json:"arch"`
	Kernel          string `json:"kernel,omitempty"`
	Filesystem      string `json:"filesystem,omitempty"`
	GoVersion       string `json:"go_version"`
	SpecVersion     uint16 `json:"spec_version"`
	BenchVersion    int    `json:"bench_version"`
	GitSHA          string `json:"git_sha,omitempty"`
	ClientCPU       int    `json:"client_cpu"`
	DaemonCPU       int    `json:"daemon_cpu,omitempty"`
	CacheState      string `json:"cache_state"`
	ColdVerified    bool   `json:"cold_verified,omitempty"`
	TimerOverheadNS int64  `json:"timer_overhead_ns,omitempty"`
}

// PhaseView is one completed stretch of a run.
type PhaseView struct {
	Name     string `json:"name"`
	Duration string `json:"duration"`
	Bytes    int64  `json:"bytes"`
}

// MetricsView is the measured outcome; open-loop runs fill both latency histograms.
type MetricsView struct {
	ThroughputMsgs     float64          `json:"throughput_msgs_per_sec"`
	ThroughputBytes    float64          `json:"throughput_bytes_per_sec"`
	LatencyCorrected   HistogramView    `json:"latency_corrected"`
	LatencyUncorrected HistogramView    `json:"latency_uncorrected"`
	TailSamplesNS      []int64          `json:"tail_samples_ns,omitempty"`
	Verified           bool             `json:"verified"`
	BoundFired         string           `json:"bound_fired"`
	Messages           int64            `json:"messages"`
	Recovery           *RecoveryMetrics `json:"recovery,omitempty"`
}

// RecoveryMetrics times the three milestones of a crash-recovery run.
type RecoveryMetrics struct {
	TimeToOpenNS         int64 `json:"time_to_open_ns"`
	TimeToFirstReadNS    int64 `json:"time_to_first_read_ns"`
	TimeToFullRecoveryNS int64 `json:"time_to_full_recovery_ns"`
}

// HistogramView is a latency summary in nanoseconds.
type HistogramView struct {
	P50  int64 `json:"p50_ns"`
	P90  int64 `json:"p90_ns"`
	P95  int64 `json:"p95_ns"`
	P99  int64 `json:"p99_ns"`
	P999 int64 `json:"p999_ns"`
	Max  int64 `json:"max_ns"`
}
