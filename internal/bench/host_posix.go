//go:build linux || darwin

package bench

import (
	"syscall"
	"time"
)

// rusageStats returns the process's peak RSS in bytes and cumulative CPU nanoseconds.
func rusageStats() (rssBytes, cpuNanos int64) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0, 0
	}
	seconds := timevalSeconds(ru.Utime) + timevalSeconds(ru.Stime)
	return int64(ru.Maxrss) * rusageRSSUnit, int64(seconds * float64(time.Second))
}

// timevalSeconds converts a timeval to seconds.
func timevalSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}
