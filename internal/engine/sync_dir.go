package engine

import "os"

// Directory fsyncs cover five durability sites: a newly created segment file,
// offset-store compaction's rename, queues.json's rename, an active segment's
// close-time truncation, and a queue key store's rename (encrypted queues only).

// SyncDirHook, when non-nil, runs before every directory sync; tests use it to observe the four durability sites.
var SyncDirHook func()

// SyncDir makes a file's directory entry durable after it is created or renamed.
func SyncDir(root *os.Root) error { return syncDir(root) }

// syncDir syncs a directory root; delegates to the OS-specific implementation.
func syncDir(root *os.Root) error {
	if SyncDirHook != nil {
		SyncDirHook()
	}
	return syncDirOS(root)
}
