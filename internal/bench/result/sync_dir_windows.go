//go:build windows

package result

import "os"

// syncDirOS is a no-op on Windows, where a directory handle cannot be fsynced.
func syncDirOS(*os.Root) error { return nil }
