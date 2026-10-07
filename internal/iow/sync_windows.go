//go:build windows

package iow

import "os"

// syncDataDefault flushes the file through the Windows filesystem cache.
func syncDataDefault(f *os.File) error {
	return f.Sync()
}