//go:build linux

package engine

import (
	"fmt"
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

// FS_IOC_SET_ENCRYPTION_POLICY ioctl constant for Linux filesystems (ext4/f2fs/btrfs)
const fsIocSetEncryptionPolicy = 0x800c6613

type fscryptPolicy struct {
	version                 uint8
	contentsEncryptionMode  uint8
	filenamesEncryptionMode uint8
	flags                   uint8
	masterKeyDescriptor     [8]byte
}

// EnsureDirectoryEncrypted configures kernel-level fscrypt directory policies on Linux.
func EnsureDirectoryEncrypted(dirPath string) error {
	f, err := os.Open(dirPath)
	if err != nil {
		return fmt.Errorf("open directory for encryption: %w", err)
	}
	defer f.Close()

	policy := fscryptPolicy{
		version:                 0,
		contentsEncryptionMode:  unix.FS_ENCRYPTION_MODE_AES_256_XTS,
		filenamesEncryptionMode: unix.FS_ENCRYPTION_MODE_AES_256_CTS,
		flags:                   0,
	}

	_, _, errno := unix.Syscall(
		unix.SYS_IOCTL,
		f.Fd(),
		fsIocSetEncryptionPolicy,
		uintptr(unsafe.Pointer(&policy)),
	)
	if errno != 0 && errno != unix.ENOTTY && errno != unix.EOPNOTSUPP && errno != unix.EEXIST {
		return fmt.Errorf("fscrypt ioctl failed for %s: %w", dirPath, errno)
	}

	return nil
}
