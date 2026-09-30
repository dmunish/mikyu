//go:build !linux && !darwin && !windows

package bench

// readPlatform reports zeros on platforms without a process-stat reader.
func readPlatform() HostStats { return HostStats{} }
