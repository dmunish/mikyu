//go:build !windows

package repl

// init is a no-op on POSIX: virtual terminal setup is not needed.
func init() {}
