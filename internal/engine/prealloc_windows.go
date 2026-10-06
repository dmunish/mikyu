//go:build windows

package engine

import (
	"os"
)

func preallocate(f *os.File, size int64) error {
	if err := f.Truncate(size); err != nil {
		return nil // best-effort on Windows; do not fail the queue
	}
	return nil
}
