//go:build !linux && !windows

package engine

import "log/slog"

// EnsureDirectoryEncrypted provides a safe fallback for systems without native transparent directory policies (e.g. macOS/Darwin, BSD).
func EnsureDirectoryEncrypted(dirPath string) error {
	slog.Warn("transparent directory encryption was requested but is not supported by this host; continuing without it", "dir", dirPath)
	return nil
}
