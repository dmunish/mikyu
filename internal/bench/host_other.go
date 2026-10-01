//go:build !linux && !darwin && !windows

package bench

// readPlatform reports zeros on platforms without a process-stat reader.
func readPlatform() HostStats { return HostStats{} }

// dropCache reports an unverified cache state on platforms without a cache-eviction primitive.
func dropCache(string) (bool, error) { return false, nil }
