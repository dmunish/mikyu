package benchview

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const sampleDir = "./bench/bench-177261981234567890"

// sampleResult is a complete run with a measured outcome.
func sampleResult() result.Result {
	started := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return result.Result{
		Version:   result.CurrentVersion,
		RunID:     "bench-177261981234567890",
		Mode:      "produce",
		Access:    "embedded",
		StartedAt: started,
		EndedAt:   started.Add(5 * time.Second),
		Complete:  true,
		Spec: spec.Spec{
			Storage: spec.StorageSpec{Durability: new(spec.Durability(spec.DurabilitySync))},
		},
		Workload: result.WorkloadView{
			Kind: "log", Queues: 1, MsgSize: 256, Duration: "5s",
			Workers: 8, Batch: 1, Warmup: "0s", Seed: 42,
		},
		Environment: result.EnvironmentView{CacheState: "warm"},
		Phases:      []result.PhaseView{{Name: "measure", Duration: "5s", Bytes: 1 << 30}},
		Metrics: &result.MetricsView{
			ThroughputMsgs:  240_123,
			ThroughputBytes: 61_471_488,
			LatencyUncorrected: result.HistogramView{
				P50: 42_000, P90: 180_000, P95: 400_000, P99: 1_200_000, P999: 4_000_000, Max: 8_100_000,
			},
			LatencyCorrected: result.HistogramView{
				P50: 42_000, P90: 180_000, P95: 400_000, P99: 1_200_000, P999: 4_000_000, Max: 8_100_000,
			},
			BoundFired: "duration",
			Messages:   1_200_000,
		},
	}
}

// renderSummary writes one run with the plain theme.
func renderSummary(t *testing.T, r result.Result) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, Summary(&buf, r, sampleDir, style.PlainTheme(), human.Formatter{}))
	return buf.String()
}

// writeRun writes a result into a fresh temp dir and returns the dir.
func writeRun(t *testing.T, r result.Result) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, result.WriteResult(dir, r))
	return dir
}

func TestSummaryGolden(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Summary(&buf, sampleResult(), sampleDir, style.PlainTheme(), human.Formatter{}))
	golden(t, "summary.txt", buf.Bytes())
}

func TestSummaryIncomplete(t *testing.T) {
	r := sampleResult()
	r.Complete = false
	r.Aborted = "context canceled"
	assert.Contains(t, renderSummary(t, r), "incomplete — context canceled")

	r.Aborted = ""
	assert.Contains(t, renderSummary(t, r), "the run was killed externally")
}

func TestSummaryWithoutMetrics(t *testing.T) {
	r := sampleResult()
	r.Metrics = nil
	out := renderSummary(t, r)
	assert.Contains(t, out, r.RunID)
	assert.NotContains(t, out, "latency")
}

func TestSummaryRecoveryRun(t *testing.T) {
	r := sampleResult()
	r.Workload.Duration, r.Workload.CrashAfter = "", 256<<10
	r.Metrics.Recovery = &result.RecoveryMetrics{TimeToOpenNS: 1_000_000, TimeToFirstReadNS: 2_000_000, TimeToFullRecoveryNS: 3_000_000}
	out := renderSummary(t, r)
	assert.Contains(t, out, "recovery")
	assert.Contains(t, out, "open 1ms · first read 2ms · full 3ms")
}

func TestShowMissingFiles(t *testing.T) {
	require.ErrorIs(t, Show(io.Discard, t.TempDir(), style.PlainTheme(), human.Formatter{}), result.ErrNotARun)
}

func TestShowRoundTrip(t *testing.T) {
	dir := writeRun(t, sampleResult())
	var buf bytes.Buffer
	require.NoError(t, Show(&buf, dir, style.PlainTheme(), human.Formatter{}))
	assert.Contains(t, buf.String(), sampleResult().RunID)
	assert.Contains(t, buf.String(), dir, "the written path is the directory read")
}
