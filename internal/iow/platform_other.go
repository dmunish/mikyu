package iow

import "os"

// newPlatformWriter reports whether the platform offers a specialization; Phase 1 has none.
func newPlatformWriter(files []*os.File, opts Options) (Writer, bool) {
	return nil, false
}