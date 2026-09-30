package benchview

import (
	"bytes"
	"io"
	"testing"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompareRefusesMixedModes(t *testing.T) {
	a := writeRun(t, sampleResult())
	b := sampleResult()
	b.RunID, b.Mode = "bench-2", "consume"
	bDir := writeRun(t, b)

	err := Compare(io.Discard, []string{a, bDir}, style.PlainTheme(), human.Formatter{})
	require.ErrorIs(t, err, result.ErrCompareMismatch)
	require.ErrorIs(t, err, bench.ErrCompareMismatch, "the bench alias must match")
}

func TestCompareMarksDifferences(t *testing.T) {
	a := writeRun(t, sampleResult())
	b := sampleResult()
	b.RunID, b.Access, b.AccessTarget = "bench-2", "daemon", "tcp://127.0.0.1:1"
	b.Workload.Workers = 4
	bDir := writeRun(t, b)

	var buf bytes.Buffer
	require.NoError(t, Compare(&buf, []string{a, bDir}, style.PlainTheme(), human.Formatter{}))
	out := buf.String()
	assert.Contains(t, out, "* access differs")
	assert.Contains(t, out, "workers differs")
	assert.Contains(t, out, "uses embedded")
	assert.Contains(t, out, "uses daemon")
	assert.Contains(t, out, "240,123 msg/s")
}

func TestCompareSingleRun(t *testing.T) {
	a := writeRun(t, sampleResult())
	var buf bytes.Buffer
	require.NoError(t, Compare(&buf, []string{a}, style.PlainTheme(), human.Formatter{}))
	assert.Contains(t, buf.String(), "1 runs")
	assert.NotContains(t, buf.String(), "differs")
}

func TestCompareNeedsRuns(t *testing.T) {
	require.ErrorIs(t, Compare(io.Discard, nil, style.PlainTheme(), human.Formatter{}), result.ErrCompareMismatch)
}
