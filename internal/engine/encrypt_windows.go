//go:build windows

package engine

import (
	"fmt"
	"unsafe"

	"golang.org/x/sys/windows"
)

var (
	modadvapi32      = windows.NewLazySystemDLL("advapi32.dll")
	procEncryptFileW = modadvapi32.NewProc("EncryptFileW")
)

// EnsureDirectoryEncrypted applies NTFS Encrypting File System (EFS) attributes to a directory.
func EnsureDirectoryEncrypted(dirPath string) error {
	pathPtr, err := windows.UTF16PtrFromString(dirPath)
	if err != nil {
		return fmt.Errorf("invalid path encoding for %s: %w", dirPath, err)
	}

	// EncryptFileW(lpFileName *uint16) BOOL -> returns non-zero on success
	r1, _, err := procEncryptFileW.Call(uintptr(unsafe.Pointer(pathPtr)))
	if r1 == 0 {
		if err != nil && err != windows.ERROR_SUCCESS {
			return fmt.Errorf("windows EFS encryption failed for %s: %w", dirPath, err)
		}
		return fmt.Errorf("windows EFS encryption failed for %s", dirPath)
	}

	return nil
}
