package command_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// benchRunFlags are the presentation flags every run registers.
var benchRunFlags = []string{
	"mode", "dir", "target", "access", "out", "persist", "cleanup-stale",
	"quiet", "verify", "cold", "cpu", "metric", "file",
}

func TestResolveVerbFindsSubcommands(t *testing.T) {
	require.True(t, command.HasVerb("bench"))
	require.True(t, command.HasVerb("bench", "run"))
	assert.False(t, command.HasVerb("bench", "nope"))

	v, path, rest, ok := command.ResolveVerb("bench", []string{"run", "--bytes", "1"})
	require.True(t, ok)
	assert.Equal(t, "run", v.Name)
	assert.Equal(t, "bench run", path)
	assert.Equal(t, []string{"--bytes", "1"}, rest)

	v, path, rest, ok = command.ResolveVerb("bench", []string{"--help"})
	require.True(t, ok)
	assert.Equal(t, "bench", v.Name)
	assert.Equal(t, "bench", path)
	assert.Equal(t, []string{"--help"}, rest)

	_, path, _, ok = command.ResolveVerb("bench", []string{"nope"})
	require.True(t, ok, "an unknown child falls back to the parent")
	assert.Equal(t, "bench", path)

	_, _, _, ok = command.ResolveVerb("nope", nil)
	assert.False(t, ok)
}

func TestBenchHasNoPerModeSubcommands(t *testing.T) {
	for _, mode := range bench.Modes {
		assert.False(t, command.HasVerb("bench", string(mode)),
			"a mode is a flag, not a subcommand")
	}
}

func TestVerbNamesFlattensSubcommands(t *testing.T) {
	names := command.VerbNames()
	for _, want := range []string{"bench", "bench run", "bench show", "bench compare", "bench plot"} {
		assert.Contains(t, names, want)
	}
}

func TestBenchFlagSetCoversEveryParam(t *testing.T) {
	fs := command.BenchFlagSet(&command.BenchArgs{})
	for _, p := range bench.Table {
		assert.NotNil(t, fs.Lookup(p.FlagName), "bench run is missing --%s", p.FlagName)
	}
	for _, p := range spec.Params(spec.LayerSpec) {
		assert.NotNil(t, fs.Lookup(p.FlagName), "bench run is missing --%s", p.FlagName)
	}
	for _, name := range benchRunFlags {
		assert.NotNil(t, fs.Lookup(name), "bench run is missing --%s", name)
	}
}

// TestBenchFlagSetCarriesEveryMode proves one flag set serves all four modes.
func TestBenchFlagSetCarriesEveryMode(t *testing.T) {
	fs := command.BenchFlagSet(&command.BenchArgs{})
	for _, name := range []string{"mode", "preload", "rate", "rate-schedule", "keys", "ack-rate", "crash-after-bytes"} {
		assert.NotNil(t, fs.Lookup(name), "bench run is missing --%s", name)
	}
}

func TestBenchHelpListsEveryWorkloadFlag(t *testing.T) {
	v, ok := command.LookupVerb("bench", "run")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	assert.Contains(t, out, bench.ModeDoc)
	assert.Contains(t, out, "--mode")
	for _, p := range bench.Table {
		assert.Contains(t, out, "--"+p.FlagName, "bench run help is missing --%s", p.FlagName)
		assert.Contains(t, out, p.Doc, "bench run help is missing the doc for %s", p.Key)
	}
	for _, name := range benchRunFlags {
		assert.Contains(t, out, "--"+name, "bench run help is missing --%s", name)
	}
}

// TestBenchHelpMarksModeScopedFlags keeps one help page honest about which mode reads a flag.
func TestBenchHelpMarksModeScopedFlags(t *testing.T) {
	v, ok := command.LookupVerb("bench", "run")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	assert.Contains(t, out, "(consume only)")
	assert.Contains(t, out, "(produce|mix only)")
	assert.Contains(t, out, "(recovery only)")
}

func TestBenchHelpListsItsSubcommands(t *testing.T) {
	v, ok := command.LookupVerb("bench")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()
	for _, sub := range v.Subs {
		assert.Contains(t, out, sub.Name)
		assert.Contains(t, out, sub.Summary)
	}
}

func TestBenchHelpReportsTheTunedDefaults(t *testing.T) {
	v, ok := command.LookupVerb("bench", "run")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	assert.Contains(t, out, "(default produce)", "produce is the mode a run starts in")
	assert.Contains(t, out, "(default async)", "a run flushes in the background by default")
	assert.Contains(t, out, "(default 256 MiB)", "segments are sized for sustained traffic")
	assert.Contains(t, out, "(default 1 KiB)", "message and byte defaults read in units")
	assert.Contains(t, out, "(default 64)", "appends are batched by default")
	assert.NotContains(t, out, "(default 64 MiB)", "the built-in segment size is not the run's")
}

// TestBenchOutputIsHumanScaledInAPipe proves a bench summary reads as units, not as bare numbers, off a terminal.
func TestBenchOutputIsHumanScaledInAPipe(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, command.Present(&buf, result.Result{
		Version: result.CurrentVersion,
		RunID:   "bench-human",
		Mode:    "produce",
		Access:  "embedded",
		Spec:    bench.DefaultSpec(),
		Workload: result.WorkloadView{
			Kind: "log", MsgSize: 1024, Bytes: 1 << 30, Workers: 4, Batch: 64,
		},
		Metrics: &result.MetricsView{
			ThroughputMsgs:  240123,
			ThroughputBytes: 245 << 20,
			BoundFired:      "bytes",
			LatencyUncorrected: result.HistogramView{
				P50: 42_000, P90: 180_000, P99: 1_200_000, Max: 8_100_000,
			},
		},
	}, "./bench/bench-human"))

	out := buf.String()
	assert.Contains(t, out, "256.0 MiB segment")
	assert.Contains(t, out, "128.0 MiB cache")
	assert.Contains(t, out, "1.0 KiB × 1.0 GiB")
	assert.Contains(t, out, "240,123")
	assert.Contains(t, out, "245.0 MiB/s")
	assert.Contains(t, out, "p50 42µs")
	assert.Contains(t, out, "p99 1.2ms")
	assert.NotContains(t, out, "\x1b[", "a pipe still gets no color")
}

func TestShowRunReportsAMissingEnvelope(t *testing.T) {
	var buf bytes.Buffer
	require.ErrorIs(t, command.ShowRun(&buf, t.TempDir()), result.ErrNotARun)
}

func TestShowRunRefusesAnUnknownEnvelopeVersion(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "result.json"), []byte(`{"version":999}`), 0o644))

	var buf bytes.Buffer
	require.ErrorIs(t, command.ShowRun(&buf, dir), result.ErrEnvelopeVersion)
}

func TestRunBenchWritesASummaryAndNoProgressBar(t *testing.T) {
	dataDir, outDir := t.TempDir(), t.TempDir()
	var stdout, stderr bytes.Buffer

	run, err := command.RunBench(t.Context(),
		[]string{
			"--dir", dataDir, "--out", outDir,
			"--bytes", "65536", "--warmup", "0s", "--workers", "1",
			"--durability", "async",
		},
		&stdout, &stderr, spec.BuiltinSession())
	require.NoError(t, err)

	assert.True(t, result.IsRun(run), "a run must leave a readable envelope behind")
	assert.Equal(t, filepath.Join(outDir, filepath.Base(run)), run, "a run lives in its own directory")

	out := stdout.String()
	assert.Contains(t, out, filepath.Base(run))
	assert.Contains(t, out, "spec")
	assert.Contains(t, out, "MSGS/S")
	assert.NotContains(t, stderr.String(), "\x1b[", "a pipe never gets a progress bar")
}

// TestRunBenchDefaultsToProduce proves the mode flag is optional on the happy path.
func TestRunBenchDefaultsToProduce(t *testing.T) {
	var stdout, stderr bytes.Buffer
	run, err := command.RunBench(t.Context(),
		[]string{
			"--dir", t.TempDir(), "--out", t.TempDir(),
			"--bytes", "65536", "--warmup", "0s", "--workers", "1",
			"--durability", "async",
		},
		&stdout, &stderr, spec.BuiltinSession())
	require.NoError(t, err)

	res, err := result.ReadResult(run)
	require.NoError(t, err)
	assert.Equal(t, "produce", res.Mode)
}

func TestRunBenchRefusesAnUnknownAccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, err := command.RunBench(t.Context(),
		[]string{"--dir", t.TempDir(), "--duration", "1s", "--access", "carrier-pigeon"},
		&stdout, &stderr, spec.BuiltinSession())
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestRunBenchNeedsADirectoryForEmbeddedAccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, err := command.RunBench(t.Context(),
		[]string{"--duration", "1s"},
		&stdout, &stderr, spec.BuiltinSession())
	require.ErrorIs(t, err, bench.ErrUsage)
}
