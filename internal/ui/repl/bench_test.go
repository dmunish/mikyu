package repl

import (
	"testing"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeRun writes a completed envelope into a fresh directory.
func writeRun(t *testing.T, id string) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, result.WriteResult(dir, result.Result{
		Version:  result.CurrentVersion,
		RunID:    id,
		Mode:     "produce",
		Access:   "embedded",
		Complete: true,
		Spec:     spec.Builtin(),
	}))
	return dir
}

func TestREPLBenchSubHelpListsCommands(t *testing.T) {
	out := captureStdout(func() { (&replSession{}).handleBench(nil) })
	assert.Contains(t, out, "COMMANDS")
	assert.Contains(t, out, "produce")
}

func TestREPLBenchShowNeedsARunDir(t *testing.T) {
	sess := &replSession{}
	out := captureStdout(func() { sess.handleBenchShow(nil) })
	assert.Contains(t, out, "Usage: bench show")
}

func TestREPLBenchShowDefaultsToTheLastRun(t *testing.T) {
	dir := writeRun(t, "bench-repl-1")
	sess := &replSession{lastBenchRun: dir}

	out := captureStdout(func() { sess.handleBenchShow(nil) })
	assert.Contains(t, out, "bench-repl-1")
}

func TestREPLBenchShowTakesAnExplicitRunDir(t *testing.T) {
	sess := &replSession{}
	out := captureStdout(func() { sess.handleBenchShow([]string{writeRun(t, "bench-repl-2")}) })
	assert.Contains(t, out, "bench-repl-2")
}

func TestREPLBenchShowFailsOnAMissingRun(t *testing.T) {
	sess := &replSession{lastBenchRun: t.TempDir()}
	out := captureStdout(func() { sess.handleBenchShow(nil) })
	assert.Contains(t, out, "Error:")
}

func TestREPLBenchPlotNeedsARunDir(t *testing.T) {
	sess := &replSession{}
	out := captureStdout(func() { sess.handleBenchPlot(nil) })
	assert.Contains(t, out, "Usage: bench plot")
}

func TestREPLBenchCompareNeedsTwoRuns(t *testing.T) {
	sess := &replSession{}
	out := captureStdout(func() { sess.handleBenchCompare([]string{"a"}) })
	assert.Contains(t, out, "Usage: bench compare")
}

func TestREPLBenchDispatchesUnknownSubcommands(t *testing.T) {
	out := captureStdout(func() { (&replSession{}).dispatch("bench bogus") })
	assert.Contains(t, out, "Unknown subcommand")
}
