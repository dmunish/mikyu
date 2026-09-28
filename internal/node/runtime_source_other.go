//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !windows

package node

// readProcessRSS is unavailable on this platform.
func readProcessRSS() int64 { return 0 }

// readProcessVSZ is unavailable on this platform.
func readProcessVSZ() int64 { return 0 }

// readProcessThreads is unavailable on this platform.
func readProcessThreads() int64 { return 0 }

// readProcessFDs is unavailable on this platform.
func readProcessFDs() int64 { return 0 }

// readProcessUserSeconds is unavailable on this platform.
func readProcessUserSeconds() float64 { return 0 }

// readProcessSysSeconds is unavailable on this platform.
func readProcessSysSeconds() float64 { return 0 }
