//go:build !windows

package repl

func init() {
	// No‑op: virtual terminal setup is not needed on POSIX.
}
