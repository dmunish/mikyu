//go:build !darwin && !linux && !windows

package iow

import "os"

// syncDataDefault falls back to a full file sync.
func syncDataDefault(f *os.File) error {
	return f.Sync()
}