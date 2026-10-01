package bench

import (
	"sync"
	"time"
)

// HostStats is a point-in-time observation of the bench process's own resource use.
type HostStats struct {
	RSSBytes   int64
	VSZBytes   int64
	CPUPercent float64
	OpenFDs    int64
	Threads    int64
}

// hostCache throttles platform reads to one scan per second so observation does not perturb the run.
var hostCache struct {
	mu    sync.Mutex
	at    time.Time
	valid bool
	stats HostStats
}

// readHostStats returns the process's current host statistics, cached for up to one second.
func readHostStats() HostStats {
	hostCache.mu.Lock()
	defer hostCache.mu.Unlock()
	if hostCache.valid && time.Since(hostCache.at) < time.Second {
		return hostCache.stats
	}
	stats := readPlatform()
	hostCache.stats = stats
	hostCache.at = time.Now()
	hostCache.valid = true
	return stats
}

// cpuBaseline holds the previous cumulative CPU reading for the utilization delta.
var cpuBaseline struct {
	mu    sync.Mutex
	used  int64
	wall  int64
	valid bool
}

// cpuPercentDelta converts cumulative process CPU nanoseconds into a utilization percent; the first sample returns zero.
func cpuPercentDelta(usedNs int64) float64 {
	now := time.Now().UnixNano()
	cpuBaseline.mu.Lock()
	defer cpuBaseline.mu.Unlock()
	if !cpuBaseline.valid {
		cpuBaseline.used = usedNs
		cpuBaseline.wall = now
		cpuBaseline.valid = true
		return 0
	}
	deltaCPU := usedNs - cpuBaseline.used
	deltaWall := now - cpuBaseline.wall
	cpuBaseline.used = usedNs
	cpuBaseline.wall = now
	if deltaWall <= 0 {
		return 0
	}
	return 100 * float64(deltaCPU) / float64(deltaWall)
}
