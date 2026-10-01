package benchview

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/ui/style"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSeries writes rows into a fresh run dir and returns the dir.
func writeSeries(t *testing.T, rows []result.SeriesRow) string {
	t.Helper()
	dir := t.TempDir()
	w, err := result.OpenSeries(dir)
	require.NoError(t, err)
	for _, row := range rows {
		require.NoError(t, w.Append(row))
	}
	require.NoError(t, w.Close())
	return dir
}

func TestPlotRefusesUnknownMetric(t *testing.T) {
	dir := writeSeries(t, nil)
	err := Plot(io.Discard, dir, []string{"bogus"}, style.PlainTheme())
	require.ErrorIs(t, err, ErrUnknownMetric)
	assert.Contains(t, err.Error(), "supported:")
	assert.Contains(t, err.Error(), "throughput")
}

func TestPlotRendersChart(t *testing.T) {
	dir := writeSeries(t, []result.SeriesRow{
		{T: 1, Client: result.ClientCols{Throughput: 100}},
		{T: 2, Client: result.ClientCols{Throughput: 50}},
		{T: 3, Client: result.ClientCols{Throughput: 0}},
	})
	var buf bytes.Buffer
	require.NoError(t, Plot(&buf, dir, []string{"throughput"}, style.PlainTheme()))
	out := buf.String()
	assert.Contains(t, out, "throughput  peak 100")
	assert.Contains(t, out, "1.0s |"+strings.Repeat("=", chartWidth))
	assert.Contains(t, out, "2.0s |"+strings.Repeat("=", chartWidth/2))
	assert.Contains(t, out, "3.0s |")
}

func TestPlotDefaultsToThroughput(t *testing.T) {
	dir := writeSeries(t, []result.SeriesRow{{T: 1, Client: result.ClientCols{Throughput: 10}}})
	var buf bytes.Buffer
	require.NoError(t, Plot(&buf, dir, nil, style.PlainTheme()))
	assert.Contains(t, buf.String(), "throughput")
}

func TestPlotNeedsSeries(t *testing.T) {
	require.ErrorIs(t, Plot(io.Discard, t.TempDir(), nil, style.PlainTheme()), result.ErrNotARun)
}
