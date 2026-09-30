//go:build !windows

package bench

import (
	"errors"
	"os"
	"syscall"
)

// terminatePID asks a process to stop, treating an already-exited process as success.
func terminatePID(pid int) error {
	proc, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}
