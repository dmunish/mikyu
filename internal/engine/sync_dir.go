package engine

import "os"

// syncDir syncs a directory root; delegates to OS-specific implementation.
func syncDir(root *os.Root) error { return syncDirOS(root) }
