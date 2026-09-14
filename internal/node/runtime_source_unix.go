//go:build !windows

package node

import "syscall"

// readProcessRSS returns the process resident set size via getrusage.
func readProcessRSS() int64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	return int64(ru.Maxrss)
}

// readProcessCPU reports process CPU utilization via getrusage.
func readProcessCPU() float64 {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return 0
	}
	used := timevalNanos(ru.Utime) + timevalNanos(ru.Stime)
	return cpuPercent(used)
}

func timevalNanos(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1e9 + int64(tv.Usec)*1e3
}
