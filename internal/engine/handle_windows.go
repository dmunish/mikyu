//go:build windows

package engine

import (
	"os"

	"golang.org/x/sys/windows"
)

// openSegmentFile opens a segment file with FILE_FLAG_OVERLAPPED so positional I/O costs one syscall.
func openSegmentFile(path string, flags int, perm os.FileMode) (*os.File, error) {
	return openOverlapped(path, flags, perm)
}

// openOverlapped creates a file handle with FILE_FLAG_OVERLAPPED and wraps it in an os.File.
func openOverlapped(path string, flags int, perm os.FileMode) (*os.File, error) {
	access := uint32(windows.GENERIC_READ | windows.GENERIC_WRITE)
	if flags&os.O_WRONLY != 0 {
		access = windows.GENERIC_WRITE
	} else if flags&os.O_RDONLY != 0 && flags&os.O_RDWR == 0 {
		access = windows.GENERIC_READ
	}
	disposition := uint32(windows.OPEN_EXISTING)
	if flags&os.O_CREATE != 0 {
		disposition = windows.OPEN_ALWAYS
	}
	h, err := windows.CreateFile(
		windows.StringToUTF16Ptr(path),
		access,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		disposition,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_OVERLAPPED,
		0,
	)
	if err != nil {
		return nil, &os.PathError{Op: "createfile", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}
