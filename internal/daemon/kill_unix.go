//go:build !windows

package daemon

import (
	"errors"
	"os/exec"
	"syscall"
)

// killProcess sends SIGKILL; a process that already exited is not an error.
func killProcess(cmd *exec.Cmd) error {
	err := cmd.Process.Signal(syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return nil
	}
	return err
}
