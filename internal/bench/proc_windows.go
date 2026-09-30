//go:build windows

package bench

import (
	"errors"
	"os"
)

// terminatePID kills a process by PID, treating an already-exited process as success.
func terminatePID(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	return nil
}
