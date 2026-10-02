package cli

import (
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBenchSubHelpListsCommands(t *testing.T) {
	app := newTestApp(t)

	out := app.run(t, "bench")
	assert.Contains(t, out, "COMMANDS")
	for _, sub := range []string{"run", "show", "compare", "plot"} {
		assert.Contains(t, out, sub)
	}
}

func TestBenchRetiredModesAreNotSubcommands(t *testing.T) {
	app := newTestApp(t)

	for _, mode := range bench.Modes {
		err := app.runErr(t, "bench", string(mode))
		require.ErrorIs(t, err, daemon.ErrUsage, "%s is a --mode value, not a subcommand", mode)
		assert.Contains(t, app.errOut.String(), "unknown subcommand")
	}
}

func TestBenchUnknownSubcommandIsAUsageError(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "bench", "bogus")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "unknown subcommand")
}

func TestBenchShowRequiresARunDir(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "bench", "show")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "bench show")
}

func TestBenchCompareNeedsTwoRuns(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "bench", "compare", "one")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "bench compare")
}

func TestBenchShowReportsAMissingRun(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "bench", "show", t.TempDir())
	require.Error(t, err)
	assert.NotErrorIs(t, err, daemon.ErrUsage, "an unreadable run is not a usage mistake")
}

func TestBenchPlotNeedsARunDir(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "bench", "plot")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "bench plot")
}

func TestBenchRunWritesARunWithoutOpeningAClient(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "run",
		"--dir", app.dir, "--out", t.TempDir(),
		"--bytes", "65536", "--warmup", "0s", "--workers", "1", "--durability", "async")
	require.NoError(t, err)

	assert.Contains(t, out, "spec")
	assert.Contains(t, out, "MSGS/S")
	assert.NotContains(t, app.errOut.String(), "\x1b[", "a pipe never gets a progress bar")
	assert.NotContains(t, out, "unknown command", "the run must dispatch, not fall through")
}

func TestBenchRunConsumesThroughTheModeFlag(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "run", "--mode", "consume",
		"--dir", app.dir, "--out", t.TempDir(),
		"--preload", "65536", "--bytes", "32768", "--warmup", "0s", "--workers", "1",
		"--batch", "16", "--durability", "async", "--verify")
	require.NoError(t, err)

	assert.Contains(t, out, "spec")
	assert.Contains(t, out, "MSGS/S")
}

// TestBenchRunNeedsNoDirectoryFlag proves the run's own data directory is the fallback.
func TestBenchRunNeedsNoDirectoryFlag(t *testing.T) {
	t.Chdir(t.TempDir())
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "run",
		"--out", t.TempDir(),
		"--bytes", "65536", "--warmup", "0s", "--workers", "1")
	require.NoError(t, err)

	assert.Contains(t, out, "MSGS/S")
	assert.DirExists(t, "mikyu-bench")
}

func TestBenchRunRefusesAPositionalArgument(t *testing.T) {
	app := newTestApp(t)

	_, err := app.runRaw(t, "bench", "run", "extra", "--dir", app.dir, "--duration", "1s")
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchRunRefusesAnUnknownMode(t *testing.T) {
	app := newTestApp(t)

	_, err := app.runRaw(t, "bench", "run", "--mode", "carrier-pigeon", "--dir", app.dir, "--duration", "1s")
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchRunRefusesAFlagAnotherModeOwns(t *testing.T) {
	app := newTestApp(t)

	_, err := app.runRaw(t, "bench", "run", "--preload", "1MiB", "--dir", app.dir, "--duration", "1s")
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchRunHelpRendersTheSubcommandPage(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "run", "--help")
	require.NoError(t, err)
	assert.Contains(t, out, "--msg-size")
	assert.Contains(t, out, "--mode")
	assert.Contains(t, out, "USAGE")
}

func TestBenchDispatchesThroughTheFlagSetOnly(t *testing.T) {
	app := newTestApp(t)

	// `help` inside a run is not a verb: a bench run takes flags only.
	_, err := app.runRaw(t, "bench", "run", "help")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "positional") || strings.Contains(err.Error(), "usage"))
}
