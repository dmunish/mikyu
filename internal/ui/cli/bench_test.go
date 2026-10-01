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
	for _, sub := range []string{"produce", "consume", "mix", "recovery", "show", "compare", "plot"} {
		assert.Contains(t, out, sub)
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

func TestBenchProduceWritesARunWithoutOpeningAClient(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "produce",
		"--dir", app.dir, "--out", t.TempDir(),
		"--bytes", "65536", "--warmup", "0s", "--workers", "1", "--durability", "async")
	require.NoError(t, err)

	assert.Contains(t, out, "spec")
	assert.Contains(t, out, "MSGS/S")
	assert.NotContains(t, app.errOut.String(), "\x1b[", "a pipe never gets a progress bar")
	assert.NotContains(t, out, "unknown command", "the run must dispatch, not fall through")
}

func TestBenchProduceRefusesAPositionalArgument(t *testing.T) {
	app := newTestApp(t)

	_, err := app.runRaw(t, "bench", "produce", "extra", "--dir", app.dir, "--duration", "1s")
	require.ErrorIs(t, err, bench.ErrUsage)
}

func TestBenchShowHelpRendersTheSubcommandPage(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "bench", "produce", "--help")
	require.NoError(t, err)
	assert.Contains(t, out, "--msg-size")
	assert.Contains(t, out, "USAGE")
}

func TestBenchDispatchesThroughTheFlagSetOnly(t *testing.T) {
	app := newTestApp(t)

	// `help produce` is a queue named help, not a benchmark subcommand page.
	_, err := app.runRaw(t, "bench", "produce", "help")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "positional") || strings.Contains(err.Error(), "usage"))
}
