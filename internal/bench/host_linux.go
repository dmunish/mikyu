//go:build linux

package bench

import (
	"os"
	"strconv"
	"strings"
)

// rusageRSSUnit is getrusage's Maxrss unit in bytes on Linux.
const rusageRSSUnit int64 = 1024

// readPlatform reads RSS and CPU time from getrusage and the rest from /proc.
func readPlatform() HostStats {
	rss, cpuNanos := rusageStats()
	stats := HostStats{RSSBytes: rss, CPUPercent: cpuPercentDelta(cpuNanos)}
	if data, err := os.ReadFile("/proc/self/stat"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) >= 23 {
			if v, err := strconv.ParseInt(fields[22], 10, 64); err == nil && v >= 0 {
				stats.VSZBytes = v
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		if v, ok := procStatusValue(string(data), "Threads:"); ok {
			stats.Threads = v
		}
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		stats.OpenFDs = int64(len(entries))
	}
	return stats
}

// procStatusValue reads one integer field from a /proc status document.
func procStatusValue(status, key string) (int64, bool) {
	for line := range strings.SplitSeq(status, "\n") {
		if value, ok := strings.CutPrefix(line, key); ok {
			v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			return v, err == nil && v >= 0
		}
	}
	return 0, false
}
