//go:build linux

package node

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// rssUnitBytes converts getrusage's Maxrss unit (kilobytes on Linux) to bytes.
const rssUnitBytes int64 = 1024

// procTTL bounds how long the /proc-derived gauges are cached between scans.
const procTTL = 5 * time.Second

type procStats struct {
	vsz     int64
	threads int64
	fds     int64
}

// procCache throttles /proc reads to one scan per ttl.
var procCache struct {
	mu    sync.Mutex
	at    time.Time
	stats procStats
}

// readProcessVSZ returns the process virtual memory size in bytes.
func readProcessVSZ() int64 { return cachedProcStats().vsz }

// readProcessThreads returns the number of OS threads in the process.
func readProcessThreads() int64 { return cachedProcStats().threads }

// readProcessFDs returns the number of open file descriptors.
func readProcessFDs() int64 { return cachedProcStats().fds }

// cachedProcStats returns the /proc gauges, rescanning at most every ttl.
func cachedProcStats() procStats {
	procCache.mu.Lock()
	if time.Since(procCache.at) < procTTL {
		stats := procCache.stats
		procCache.mu.Unlock()
		return stats
	}
	procCache.mu.Unlock()

	stats := scanProcStats()
	procCache.mu.Lock()
	procCache.stats = stats
	procCache.at = time.Now()
	procCache.mu.Unlock()
	return stats
}

// scanProcStats reads VSZ, thread count, and FD count from /proc.
func scanProcStats() procStats {
	var stats procStats
	if data, err := os.ReadFile("/proc/self/stat"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) >= 23 {
			if v, err := strconv.ParseInt(fields[22], 10, 64); err == nil && v >= 0 {
				stats.vsz = v
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for line := range strings.SplitSeq(string(data), "\n") {
			value, ok := strings.CutPrefix(line, "Threads:")
			if !ok {
				continue
			}
			if v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64); err == nil && v >= 0 {
				stats.threads = v
			}
			break
		}
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		stats.fds = int64(len(entries))
	}
	return stats
}
