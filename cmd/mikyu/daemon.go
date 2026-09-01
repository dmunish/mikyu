package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/dmunish/mikyu/internal/protocol"
)

// IsDaemonAlive checks if a Mikyu daemon is actively responding on the target socket address.
func IsDaemonAlive(target string) bool {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return false
	}

	conn, err := net.DialTimeout(network, addr, 150*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// EnsureDaemonRunning checks if the daemon is active; if not, it spawns a detached background instance.
func EnsureDaemonRunning(target string) error {
	if IsDaemonAlive(target) {
		return nil
	}

	execPath, err := os.Executable()
	if err != nil {
		return fmt.Errorf("failed to locate executable path: %w", err)
	}

	cmd := exec.Command(execPath, "daemon")
	cmd.Stdout = nil
	cmd.Stderr = nil
	cmd.Stdin = nil

	// Uses platform-specific implementation from detach_unix.go / detach_windows.go
	setDetachedProcessAttr(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to spawn background daemon: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("daemon process spawned, but target %s failed to respond within timeout", target)
		case <-ticker.C:
			if IsDaemonAlive(target) {
				return nil
			}
		}
	}
}
