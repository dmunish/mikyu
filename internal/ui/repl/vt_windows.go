//go:build windows

package repl

import (
	"syscall"
	"unsafe"
)

func init() {
	kernel32 := syscall.NewLazyDLL("kernel32.dll")
	getStdHandle := kernel32.NewProc("GetStdHandle")
	getConsoleMode := kernel32.NewProc("GetConsoleMode")
	setConsoleMode := kernel32.NewProc("SetConsoleMode")

	enableVT := func(fd int) {
		handle, _, _ := getStdHandle.Call(uintptr(fd))
		var mode uint32
		if r, _, _ := getConsoleMode.Call(handle, uintptr(unsafe.Pointer(&mode))); r == 0 {
			return
		}
		mode |= 0x0004 // ENABLE_VIRTUAL_TERMINAL_PROCESSING
		mode |= 0x0001 // ENABLE_PROCESSED_OUTPUT
		setConsoleMode.Call(handle, uintptr(mode))
	}
	enableVT(syscall.STD_OUTPUT_HANDLE)
	enableVT(syscall.STD_ERROR_HANDLE)
}
