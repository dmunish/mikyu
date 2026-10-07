//go:build !windows

package iow

import "os"

// newIOCPWriter reports no completion port: overlapped I/O is a Windows kernel primitive and no other host offers an equivalent completion queue for file writes.
func newIOCPWriter(files []*os.File, opts Options) (Writer, bool) {
	return nil, false
}
