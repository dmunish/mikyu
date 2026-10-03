//go:build !windows

package engine

import "os"

// openSegmentFile opens a segment file with the standard library.
func openSegmentFile(path string, flags int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(path, flags, perm)
}
