//go:build windows

package engine

import "os"

// syncDirOS is a no-op on Windows; NTFS directory fsync has no direct equivalent.
func syncDirOS(root *os.Root) error {
	return nil
}
