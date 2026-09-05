//go:build !windows

package daemon

import (
	"os/exec"
	"syscall"
)

func setDetachedProcessAttr(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
