//go:build unix

package server

// sendfileSupported reports whether the kernel's zero-copy send primitive is usable.
func sendfileSupported() bool {
	return true
}
