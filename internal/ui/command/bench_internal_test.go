package command

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"charm.land/bubbles/v2/progress"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// envLookup reads a fixed environment so precedence can be tested without touching the real one.
func envLookup(env map[string]string) func(string) string {
	return func(name string) string { return env[name] }
}

func TestBenchEnvVarsAreHonored(t *testing.T) {
	env := map[string]string{"MIKYU_BENCH_WORKERS": "7", "MIKYU_BENCH_MSG_SIZE": "512"}

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s"},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 7, opts.Workload.Workers)
	assert.Equal(t, 512, opts.Workload.MsgSize)
}

func TestBenchEnvVarsAreScopedToTheirMode(t *testing.T) {
	env := map[string]string{"MIKYU_BENCH_PRELOAD": "1024", "MIKYU_BENCH_WORKERS": "3"}

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s"},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Zero(t, opts.Workload.Preload, "preload is consume-only")
	assert.Equal(t, 3, opts.Workload.Workers)
}

func TestBenchFlagsOverrideEnv(t *testing.T) {
	env := map[string]string{"MIKYU_BENCH_WORKERS": "7"}

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s", "--workers", "3"},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 3, opts.Workload.Workers)
}

func TestBenchScenarioOverridesEnvAndFlagsOverrideScenario(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	doc := "mode: produce\nworkload:\n  kind: log\n  queues: 1\n  msg_size: 128\n  bytes: 65536\n  warmup: 0s\n  workers: 2\n  batch: 1\n"
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o644))

	env := map[string]string{"MIKYU_BENCH_WORKERS": "7", "MIKYU_BENCH_MSG_SIZE": "512"}

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--file", path},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 2, opts.Workload.Workers, "the scenario overrides the environment")
	assert.Equal(t, 128, opts.Workload.MsgSize)

	opts, err = resolveBench(bench.ModeProduce, []string{"--dir", "d", "--file", path, "--workers", "9"},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 9, opts.Workload.Workers, "a flag overrides the scenario")
}

func TestBenchScenarioCarriesTheQueueSpec(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	doc := "mode: produce\nspec:\n  storage:\n    durability: async\nworkload:\n  kind: log\n  queues: 1\n  msg_size: 256\n  bytes: 1024\n  warmup: 0s\n  workers: 1\n  batch: 1\n"
	require.NoError(t, os.WriteFile(path, []byte(doc), 0o644))

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--file", path},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	require.NotNil(t, opts.Spec.Storage.Durability)
	assert.Equal(t, spec.DurabilityAsync, *opts.Spec.Storage.Durability)
}

func TestBenchScenarioModeMustMatchTheSubcommand(t *testing.T) {
	path := filepath.Join(t.TempDir(), "scenario.yaml")
	require.NoError(t, os.WriteFile(path, []byte("mode: consume\nworkload:\n  duration: 1s\n"), 0o644))

	_, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--file", path},
		spec.BuiltinSession(), envLookup(nil))
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchRejectsPositionalArguments(t *testing.T) {
	_, err := resolveBench(bench.ModeProduce, []string{"extra", "--dir", "d", "--duration", "1s"},
		spec.BuiltinSession(), envLookup(nil))
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchInheritsTheSessionDirectory(t *testing.T) {
	sess := spec.BuiltinSession()
	sess.DataDir = "from-session"
	sess.Target = "tcp://127.0.0.1:1"

	opts, err := resolveBench(bench.ModeProduce, []string{"--duration", "1s"}, sess, envLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, "from-session", opts.Dir)
	assert.Empty(t, opts.Target, "a bench run takes its target from --target, never from the session")

	opts, err = resolveBench(bench.ModeProduce, []string{"--duration", "1s", "--dir", "explicit"}, sess, envLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, "explicit", opts.Dir)
}

func TestBenchCleanupStaleIsOnByDefault(t *testing.T) {
	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s"},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	assert.False(t, opts.NoCleanupStale)

	opts, err = resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s", "--cleanup-stale=false"},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	assert.True(t, opts.NoCleanupStale)
}

func TestBenchMetricFlagAcceptsCommaLists(t *testing.T) {
	opts, err := resolveBench(bench.ModeProduce,
		[]string{"--dir", "d", "--duration", "1s", "--metric", "p50,p99", "--metric", "throughput"},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, []string{"p50", "p99", "throughput"}, opts.Metrics)
}

func TestSplitCSVTrimsAndDropsEmpties(t *testing.T) {
	assert.Equal(t, []string{"p50", "p99"}, splitCSV(" p50 , p99 ,"))
	assert.Nil(t, splitCSV(" , "))
}

func TestBenchByteFlagsAcceptUnits(t *testing.T) {
	opts, err := resolveBench(bench.ModeProduce,
		[]string{"--dir", "d", "--bytes", "256MB", "--msg-size", "4 KiB"},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	assert.Equal(t, int64(256_000_000), opts.Workload.Bytes)
	assert.Equal(t, 4096, opts.Workload.MsgSize)
}

func TestBenchEnvByteFlagsAcceptUnits(t *testing.T) {
	env := map[string]string{"MIKYU_BENCH_MSG_SIZE": "64KiB"}

	opts, err := resolveBench(bench.ModeProduce, []string{"--dir", "d", "--duration", "1s"},
		spec.BuiltinSession(), envLookup(env))
	require.NoError(t, err)
	assert.Equal(t, 64<<10, opts.Workload.MsgSize)
}

func TestBenchSpecFlagsAcceptUnits(t *testing.T) {
	opts, err := resolveBench(bench.ModeProduce,
		[]string{"--dir", "d", "--duration", "1s", "--max-segment", "512MiB"},
		spec.BuiltinSession(), envLookup(nil))
	require.NoError(t, err)
	require.NotNil(t, opts.Spec.Storage.MaxSegment)
	assert.Equal(t, int64(512<<20), *opts.Spec.Storage.MaxSegment)
}

func TestBenchRejectsAnUnknownByteUnit(t *testing.T) {
	_, err := resolveBench(bench.ModeProduce,
		[]string{"--dir", "d", "--bytes", "256MBB"},
		spec.BuiltinSession(), envLookup(nil))
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestClamp01BoundsTheFraction(t *testing.T) {
	assert.Equal(t, 0.0, clamp01(-2))
	assert.Equal(t, 1.0, clamp01(4))
	assert.InDelta(t, 0.25, clamp01(0.25), 0.001)
}

// newTestProgress builds a progress bar writing to buf, bypassing the TTY gate.
func newTestProgress(buf *bytes.Buffer, warmup, measure time.Duration) *benchProgress {
	return &benchProgress{
		w:       buf,
		warmup:  warmup,
		measure: measure,
		done:    make(chan struct{}),
		bar: progress.New(
			progress.WithWidth(progressWidth),
			progress.WithFillCharacters('=', '-'),
			progress.WithColors(benchBarColors...),
			progress.WithoutPercentage(),
		),
	}
}

func TestProgressBarRendersPhasesOnAnyWriter(t *testing.T) {
	var buf bytes.Buffer
	p := newTestProgress(&buf, 0, 0)

	p.event(bench.ProgressEvent{Kind: bench.ProgressStart, Phase: bench.PhaseMeasure})
	start := buf.String()
	assert.Contains(t, start, "[measure]")
	assert.NotContains(t, start, "\n", "a live phase stays on one line")

	p.event(bench.ProgressEvent{Kind: bench.ProgressTick, Phase: bench.PhaseMeasure, Bytes: 50, Total: 100})
	mid := buf.String()
	assert.Contains(t, mid, "=", "a ticked bar shows fill characters")
	assert.Contains(t, mid, "50%")

	p.event(bench.ProgressEvent{Kind: bench.ProgressEnd, Phase: bench.PhaseMeasure})
	end := buf.String()
	assert.Contains(t, end, "100%")
	assert.Contains(t, end, "\n", "a finished phase commits its line")

	p.stop()
	assert.Contains(t, buf.String(), "[measure]")
}

func TestProgressBarScalesToBytes(t *testing.T) {
	var buf bytes.Buffer
	p := newTestProgress(&buf, 0, 0)
	p.started = time.Now().Add(-time.Second)
	p.event(bench.ProgressEvent{Kind: bench.ProgressStart, Phase: bench.PhaseMeasure, Total: 200})

	p.mu.Lock()
	p.bytes = 100
	frac := p.percentLocked()
	p.mu.Unlock()
	assert.InDelta(t, 0.5, frac, 0.001, "half the byte budget is half the bar")
}

func TestProgressBarFallsBackToDuration(t *testing.T) {
	var buf bytes.Buffer
	p := newTestProgress(&buf, 0, 4*time.Second)
	p.event(bench.ProgressEvent{Kind: bench.ProgressStart, Phase: bench.PhaseMeasure})
	p.started = time.Now().Add(-2 * time.Second)

	p.mu.Lock()
	frac := p.percentLocked()
	p.mu.Unlock()
	assert.InDelta(t, 0.5, frac, 0.02, "a duration-bound measure phase tracks wall clock")
}

func TestProgressPrinterSuppressedOffTerminal(t *testing.T) {
	opts := bench.Options{}
	assert.Nil(t, startProgressPrinter(&bytes.Buffer{}, opts), "a pipe never gets a bar")

	opts.Quiet = true
	assert.Nil(t, startProgressPrinter(os.Stderr, opts), "--quiet suppresses the bar")
}
