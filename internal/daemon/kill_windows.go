//go:build windows

package daemon

import "os/exec"

// killProcess terminates the process; Windows has no graceful signal to send.
func killProcess(cmd *exec.Cmd) error { return cmd.Process.Kill() }
