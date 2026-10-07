//go:build linux

package iow

import (
	"os"

	"golang.org/x/sys/unix"
)

// syncDataDefault flushes data and metadata needed to read the file back.
func syncDataDefault(f *os.File) error {
	return unix.Fdatasync(int(f.Fd()))
}