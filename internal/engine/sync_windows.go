//go:build windows

package engine

import "os"

func syncData(f *os.File) error {
	return f.Sync()
}