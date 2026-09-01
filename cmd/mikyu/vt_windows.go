//go:build windows

package main

import (
	"golang.org/x/sys/windows"
)

func init() {
	// Request OS console handles for stdout and stderr
	for _, handleID := range []uint32{windows.STD_OUTPUT_HANDLE, windows.STD_ERROR_HANDLE} {
		handle, err := windows.GetStdHandle(handleID)
		if err != nil {
			continue
		}

		var mode uint32
		if err := windows.GetConsoleMode(handle, &mode); err == nil {
			// Enable VT processing and processed output for ANSI color escape parsing
			mode |= windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING | windows.ENABLE_PROCESSED_OUTPUT
			_ = windows.SetConsoleMode(handle, mode)
		}
	}
}
