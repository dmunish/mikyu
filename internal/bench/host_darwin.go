//go:build darwin

package bench

import "os"

// rusageRSSUnit is getrusage's Maxrss unit in bytes on Darwin.
const rusageRSSUnit int64 = 1

// readPlatform reads RSS and CPU time from getrusage and open descriptors from /dev/fd.
func readPlatform() HostStats {
	rss, cpuNanos := rusageStats()
	stats := HostStats{RSSBytes: rss, CPUPercent: cpuPercentDelta(cpuNanos)}
	if entries, err := os.ReadDir("/dev/fd"); err == nil {
		stats.OpenFDs = int64(len(entries))
	}
	return stats
}

// dropCache cannot evict the page cache on Darwin; it reports an unverified state.
func dropCache(string) (bool, error) { return false, nil }
