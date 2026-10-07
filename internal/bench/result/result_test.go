package result

import (
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite golden files")

// golden compares out against the committed testdata file, or rewrites it under -update.
// Newlines are normalized because a checkout with autocrlf rewrites the golden's LF as CRLF.
func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
		return
	}
	want, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("golden %s is missing; run with -update to create it", path)
	}
	require.NoError(t, err)
	assert.Equal(t, normalizeNewlines(want), normalizeNewlines(got))
}

func normalizeNewlines(b []byte) string {
	return strings.ReplaceAll(strings.ReplaceAll(string(b), "\r\n", "\n"), "\r", "\n")
}

// sampleResult is a fully populated envelope, complete run with recovery metrics absent.
func sampleResult() Result {
	started := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	return Result{
		Version:      CurrentVersion,
		RunID:        "bench-177261981234567890",
		Mode:         "produce",
		Access:       "embedded",
		AccessTarget: "",
		StartedAt:    started,
		EndedAt:      started.Add(5 * time.Second),
		Complete:     true,
		Spec: spec.Spec{
			Storage: spec.StorageSpec{
				Durability: new(spec.Durability(spec.DurabilitySync)),
				MaxSegment: new(int64(67108864)),
			},
		},
		Workload: WorkloadView{
			Kind:     "log",
			Queues:   1,
			MsgSize:  256,
			Duration: "5s",
			Workers:  8,
			Batch:    1,
			Seed:     42,
		},
		Environment: EnvironmentView{
			Host:            "bench-host",
			OS:              "linux",
			Arch:            "amd64",
			GoVersion:       "go1.27.0",
			SpecVersion:     spec.CurrentSpecVersion,
			BenchVersion:    CurrentVersion,
			ClientCPU:       8,
			CacheState:      "warm",
			TimerOverheadNS: 42,
		},
		Phases: []PhaseView{
			{Name: "measure", Duration: "5s", Bytes: 4096},
		},
		Metrics: &MetricsView{
			ThroughputMsgs:  240123.5,
			ThroughputBytes: 62914560,
			LatencyCorrected: HistogramView{
				P50: 42000, P90: 180000, P95: 400000, P99: 1200000, P999: 4000000, Max: 8100000,
			},
			LatencyUncorrected: HistogramView{
				P50: 40000, P90: 150000, P95: 350000, P99: 900000, P999: 3000000, Max: 7000000,
			},
			TailSamplesNS: []int64{8100000, 7000000, 4000000},
			Verified:      true,
			BoundFired:    "duration",
			Messages:      1200000,
		},
		Notes: []string{"observation enabled"},
	}
}

func TestResultGolden(t *testing.T) {
	got, err := marshalIndent(sampleResult())
	require.NoError(t, err)
	golden(t, "result.json", got)
}

// A checkout with autocrlf rewrites the golden's newlines, so the comparison must not depend on them.
func TestNormalizeNewlines(t *testing.T) {
	assert.Equal(t, "a\nb\nc\n", normalizeNewlines([]byte("a\r\nb\rc\n")))
	assert.Equal(t, "", normalizeNewlines(nil))
}

func TestResultRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := sampleResult()
	require.NoError(t, WriteResult(dir, want))

	got, err := ReadResult(dir)
	require.NoError(t, err)
	assert.Equal(t, want, got)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "the temp file must not survive an atomic write")
}

func TestWriteResultCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "run")
	require.NoError(t, WriteResult(dir, sampleResult()))
	got, err := ReadResult(dir)
	require.NoError(t, err)
	assert.Equal(t, "bench-177261981234567890", got.RunID)
}

func TestVersionCheck(t *testing.T) {
	dir := t.TempDir()
	r := sampleResult()
	r.Version = 999
	require.NoError(t, WriteResult(dir, r))

	_, err := ReadResult(dir)
	require.ErrorIs(t, err, ErrEnvelopeVersion)
}

func TestReadResultRejectsUnknownFields(t *testing.T) {
	dir := t.TempDir()
	body := `{"version":1,"run_id":"x","surprise":true}`
	require.NoError(t, os.WriteFile(filepath.Join(dir, resultFileName), []byte(body), 0o644))

	_, err := ReadResult(dir)
	require.Error(t, err)
	assert.NotErrorIs(t, err, ErrEnvelopeVersion)
}

func TestSeriesStreaming(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenSeries(dir)
	require.NoError(t, err)

	want := []SeriesRow{
		{T: 1, Phase: "populate", Client: ClientCols{MsgsInFlight: 3, Throughput: 1000.5, P50NS: 10, P90NS: 20, P99NS: 30}},
		{T: 2, Phase: "measure", Client: ClientCols{MsgsInFlight: 5, Throughput: 2000.25, P50NS: 40, P90NS: 50, P99NS: 60}},
		{T: 3, Phase: "measure", System: SystemCols{QueueDepth: 900, WALChannelFill: 0.25}, Host: HostCols{RSSBytes: 1 << 30, CPUPercent: 87.5, Threads: 32}},
	}
	for _, row := range want {
		require.NoError(t, w.Append(row))
	}
	require.NoError(t, w.Close())

	got, err := ReadSeries(dir)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestSeriesEmptyAfterHeader(t *testing.T) {
	dir := t.TempDir()
	w, err := OpenSeries(dir)
	require.NoError(t, err)
	require.NoError(t, w.Close())

	got, err := ReadSeries(dir)
	require.NoError(t, err)
	assert.Empty(t, got)

	head, err := os.ReadFile(filepath.Join(dir, seriesFileName))
	require.NoError(t, err)
	assert.Equal(t, seriesHeader, string(head))
}

func TestReadSeriesRejectsShortRow(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, seriesFileName), []byte(seriesHeader+"1,measure\n"), 0o644))

	_, err := ReadSeries(dir)
	require.Error(t, err)
}

func TestIsRun(t *testing.T) {
	dir := t.TempDir()
	assert.False(t, IsRun(dir))

	require.NoError(t, WriteResult(dir, sampleResult()))
	assert.False(t, IsRun(dir), "result.json alone is not a run")

	w, err := OpenSeries(dir)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	assert.True(t, IsRun(dir))

	assert.False(t, IsRun(filepath.Join(dir, "absent")))
}

func TestReadResultMissing(t *testing.T) {
	_, err := ReadResult(t.TempDir())
	require.ErrorIs(t, err, ErrNotARun)

	_, err = ReadSeries(t.TempDir())
	require.ErrorIs(t, err, ErrNotARun)
}
