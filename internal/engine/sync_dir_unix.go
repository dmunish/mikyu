//go:build !windows

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDirOS fsyncs a directory through its root file descriptor.
func syncDirOS(root *os.Root) error {
	dir, err := root.Open(".")
	if err != nil {
		return err
	}
	defer dir.Close()
	return unix.Fsync(int(dir.Fd()))
}
