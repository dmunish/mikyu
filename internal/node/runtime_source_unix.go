//go:build linux || darwin || freebsd || netbsd || openbsd

package node

import "syscall"

// readProcessRSS returns the process peak resident set size in bytes.
func readProcessRSS() int64 {
	ru, ok := readRusage()
	if !ok {
		return 0
	}
	return int64(ru.Maxrss) * rssUnitBytes
}

// readProcessUserSeconds returns cumulative user CPU time in seconds.
func readProcessUserSeconds() float64 {
	ru, ok := readRusage()
	if !ok {
		return 0
	}
	return timevalSeconds(ru.Utime)
}

// readProcessSysSeconds returns cumulative system CPU time in seconds.
func readProcessSysSeconds() float64 {
	ru, ok := readRusage()
	if !ok {
		return 0
	}
	return timevalSeconds(ru.Stime)
}

// readRusage fetches the process resource usage counters.
func readRusage() (syscall.Rusage, bool) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return syscall.Rusage{}, false
	}
	return ru, true
}

// timevalSeconds converts a timeval to seconds.
func timevalSeconds(tv syscall.Timeval) float64 {
	return float64(tv.Sec) + float64(tv.Usec)/1e6
}
