package bench

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadHostStats(t *testing.T) {
	stats := readHostStats()
	assert.GreaterOrEqual(t, stats.RSSBytes, int64(0))
	assert.GreaterOrEqual(t, stats.VSZBytes, int64(0))
	assert.GreaterOrEqual(t, stats.OpenFDs, int64(0))
	assert.GreaterOrEqual(t, stats.Threads, int64(0))
	assert.GreaterOrEqual(t, stats.CPUPercent, 0.0)

	switch runtime.GOOS {
	case "linux", "darwin":
		assert.Positive(t, stats.RSSBytes, "RSS must be readable on %s", runtime.GOOS)
	case "windows":
		assert.Positive(t, stats.RSSBytes, "RSS must be readable on windows")
	}
}
