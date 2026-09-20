//go:build !windows

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDirOS fsyncs a directory through its root file descriptor.
func syncDirOS(root *os.Root) error {
	return unix.Fsync(int(root.Fd()))
}