//go:build darwin

package iow

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDataDefault forces the drive cache to disk on macOS.
func syncDataDefault(f *os.File) error {
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	if err != nil {
		return f.Sync()
	}
	return nil
}