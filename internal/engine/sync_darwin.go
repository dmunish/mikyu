//go:build darwin

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncData(f *os.File) error {
	// F_FULLFSYNC forces physical drive cache flushing on macOS
	_, err := unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)
	if err != nil {
		return f.Sync()
	}
	return nil
}
