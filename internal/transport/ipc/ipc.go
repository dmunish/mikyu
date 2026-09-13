package ipc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// ErrUnsupportedNetwork is returned when a target resolves to a network other than unix or pipe.
var ErrUnsupportedNetwork = errors.New("unsupported IPC network type")

// Listener wraps a net.Listener to ensure filesystem socket files are cleaned up on Close.
type Listener struct {
	net.Listener
	network string
	path    string
}

// Listen creates a specialized local IPC listener for Unix Domain Sockets or Windows Named Pipes.
func Listen(target string) (net.Listener, error) {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, err
	}

	if network != "unix" && network != "pipe" {
		return nil, fmt.Errorf("%w %q (expected unix or pipe)", ErrUnsupportedNetwork, network)
	}

	if network == "unix" {
		return listenUnix(addr)
	}

	return listenPipe(addr)
}

// listenUnix handles Unix Domain Socket creation, removing stale socket files if present.
func listenUnix(path string) (net.Listener, error) {
	// Ensure parent directory exists
	if dir := filepath.Dir(path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("failed to create socket directory: %w", err)
		}
	}

	// Unlink stale socket file if left behind by an ungraceful crash
	if info, err := os.Stat(path); err == nil && !info.IsDir() {
		_ = os.Remove(path)
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, fmt.Errorf("failed to bind unix domain socket at %s: %w", path, err)
	}

	// Restrict socket access to owner read/write for security
	_ = os.Chmod(path, 0o600)

	return &Listener{
		Listener: ln,
		network:  "unix",
		path:     path,
	}, nil
}

// listenPipe handles Windows Named Pipes or falls back to Unix Domain Sockets on POSIX systems.
func listenPipe(path string) (net.Listener, error) {
	if runtime.GOOS == "windows" {
		if !strings.HasPrefix(path, `\\.\pipe\`) {
			path = `\\.\pipe\` + strings.TrimPrefix(path, `\\.\pipe\`)
		}
		ln, err := net.Listen("unix", path)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on windows pipe at %s: %w", path, err)
		}
		return &Listener{
			Listener: ln,
			network:  "pipe",
			path:     path,
		}, nil
	}

	return listenUnix(path)
}

// Dial connects to a local IPC target (unix:// or pipe://).
func Dial(target string) (net.Conn, error) {
	return DialContext(context.Background(), target)
}

// DialContext connects to a local IPC target (unix:// or pipe://) with context support.
func DialContext(ctx context.Context, target string) (net.Conn, error) {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, err
	}

	if network != "unix" && network != "pipe" {
		return nil, fmt.Errorf("%w %q (expected unix or pipe)", ErrUnsupportedNetwork, network)
	}

	if network == "pipe" && runtime.GOOS == "windows" {
		if !strings.HasPrefix(addr, `\\.\pipe\`) {
			addr = `\\.\pipe\` + strings.TrimPrefix(addr, `\\.\pipe\`)
		}
		network = "unix"
	} else if network == "pipe" {
		network = "unix"
	}

	var d net.Dialer
	return d.DialContext(ctx, network, addr)
}

// Close closes the underlying network listener and unlinks the IPC socket file from disk.
func (l *Listener) Close() error {
	err := l.Listener.Close()
	if l.network == "unix" && l.path != "" {
		_ = os.Remove(l.path)
	}
	return err
}
