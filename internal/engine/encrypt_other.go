//go:build !linux && !windows

package engine

import "log"

// EnsureDirectoryEncrypted provides a safe fallback for systems without native transparent directory policies (e.g. macOS/Darwin, BSD).
func EnsureDirectoryEncrypted(dirPath string) error {
	log.Printf("[WARN] Transparent directory encryption was requested for %s, but is not natively supported by the host OS kernel.", dirPath)
	return nil
}
