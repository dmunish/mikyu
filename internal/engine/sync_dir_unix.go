//go:build !windows

package engine

import "os"

// syncDirOS fsyncs a directory through its root file descriptor.
func syncDirOS(root *os.Root) error {
	return syncData(root.Fd())
}
