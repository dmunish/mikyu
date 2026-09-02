//go:build linux || freebsd || netbsd || openbsd

package engine

import (
	"os"

	"golang.org/x/sys/unix"
)

func syncData(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}
