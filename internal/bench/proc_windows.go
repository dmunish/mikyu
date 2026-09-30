//go:build windows

package bench

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
)

// spawnDetached re-executes this binary with args, detached from the parent and marked as the bench child.
func spawnDetached(args []string) (*os.Process, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	cmd := exec.Command(exe, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000} // CREATE_NO_WINDOW
	cmd.Env = append(os.Environ(), childEnvName+"=1")
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return cmd.Process, nil
}

// kill forcibly terminates a spawned child and reaps it, treating an already-exited child as success.
func kill(p *os.Process) error {
	if err := p.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return err
	}
	_, _ = p.Wait()
	return nil
}

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
