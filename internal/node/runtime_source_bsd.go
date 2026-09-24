//go:build freebsd || netbsd || openbsd

package node

// rssUnitBytes converts getrusage's Maxrss unit (kilobytes on the BSDs) to bytes.
const rssUnitBytes int64 = 1024

// readProcessVSZ is unavailable without cgo on the BSDs.
func readProcessVSZ() int64 { return 0 }

// readProcessThreads is unavailable without cgo on the BSDs.
func readProcessThreads() int64 { return 0 }

// readProcessFDs is not worth the syscall on the BSDs.
func readProcessFDs() int64 { return 0 }
