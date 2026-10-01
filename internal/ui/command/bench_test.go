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

// benchRunFlags are the presentation flags every run mode registers.
var benchRunFlags = []string{
	"dir", "target", "access", "out", "persist", "cleanup-stale",
	"quiet", "verify", "cold", "cpu", "metric", "file",
}

func TestResolveVerbFindsSubcommands(t *testing.T) {
	require.True(t, command.HasVerb("bench"))
	require.True(t, command.HasVerb("bench", "produce"))
	assert.False(t, command.HasVerb("bench", "nope"))

	v, path, rest, ok := command.ResolveVerb("bench", []string{"produce", "--bytes", "1"})
	require.True(t, ok)
	assert.Equal(t, "produce", v.Name)
	assert.Equal(t, "bench produce", path)
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

func TestVerbNamesFlattensSubcommands(t *testing.T) {
	names := command.VerbNames()
	for _, want := range []string{"bench", "bench produce", "bench recovery", "bench show", "bench plot"} {
		assert.Contains(t, names, want)
	}
}

func TestBenchFlagSetCoversEveryParam(t *testing.T) {
	for _, mode := range bench.Modes {
		fs := command.BenchFlagSet(mode, &command.BenchArgs{})
		for _, p := range bench.Table {
			if !p.AppliesTo(mode) {
				continue
			}
			assert.NotNil(t, fs.Lookup(p.FlagName), "bench %s is missing --%s", mode, p.FlagName)
		}
		for _, p := range spec.Params(spec.LayerSpec) {
			assert.NotNil(t, fs.Lookup(p.FlagName), "bench %s is missing --%s", mode, p.FlagName)
		}
		for _, name := range benchRunFlags {
			assert.NotNil(t, fs.Lookup(name), "bench %s is missing --%s", mode, name)
		}
	}
}

func TestBenchFlagSetScopesModeRows(t *testing.T) {
	produce := command.BenchFlagSet(bench.ModeProduce, &command.BenchArgs{})
	assert.Nil(t, produce.Lookup("preload"), "preload is consume-only")
	assert.Nil(t, produce.Lookup("crash-after-bytes"), "crash-after-bytes is recovery-only")

	consume := command.BenchFlagSet(bench.ModeConsume, &command.BenchArgs{})
	assert.Nil(t, consume.Lookup("rate"), "rate is produce-only")
	assert.NotNil(t, consume.Lookup("preload"))
	assert.NotNil(t, consume.Lookup("ack-rate"))
}

func TestBenchHelpListsEveryWorkloadFlag(t *testing.T) {
	v, ok := command.LookupVerb("bench", "produce")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	for _, p := range bench.Table {
		if !p.AppliesTo(bench.ModeProduce) {
			continue
		}
		assert.Contains(t, out, "--"+p.FlagName, "bench produce help is missing --%s", p.FlagName)
		assert.Contains(t, out, p.Doc, "bench produce help is missing the doc for %s", p.Key)
	}
	for _, name := range benchRunFlags {
		assert.Contains(t, out, "--"+name, "bench produce help is missing --%s", name)
	}
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

	run, err := command.RunBench(t.Context(), bench.ModeProduce,
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

func TestRunBenchRefusesAnUnknownAccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, err := command.RunBench(t.Context(), bench.ModeProduce,
		[]string{"--dir", t.TempDir(), "--duration", "1s", "--access", "carrier-pigeon"},
		&stdout, &stderr, spec.BuiltinSession())
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestRunBenchNeedsADirectoryForEmbeddedAccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, err := command.RunBench(t.Context(), bench.ModeProduce,
		[]string{"--duration", "1s"},
		&stdout, &stderr, spec.BuiltinSession())
	require.ErrorIs(t, err, bench.ErrUsage)
}
