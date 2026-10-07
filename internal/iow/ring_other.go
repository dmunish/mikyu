//go:build !linux

package iow

import "os"

// newRingWriter reports no ring: io_uring is Linux-only, and no other host has a
// kernel primitive that completes an asynchronous write to a regular file — macOS
// kqueue states outright that EVFILT_AIO is unsupported, and the BSDs never wired
// EVFILT_AIO up. The pool is the correct answer on those platforms, not a fallback.
func newRingWriter(files []*os.File, opts Options) (Writer, bool) {
	return nil, false
}
