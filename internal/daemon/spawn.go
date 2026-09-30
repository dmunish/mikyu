package daemon

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"sync"
	"time"
)

// Process is a handle to a daemon this process spawned.
type Process struct {
	PID    int
	Target string

	cmd     *exec.Cmd
	mu      sync.Mutex
	killed  bool
	waited  bool
	waitErr error
}

// Spawn launches a detached daemon and returns as soon as the process exists; it does not wait for readiness.
func Spawn(opts SpawnOptions) (*Process, error) {
	target := opts.Target
	if target == "" {
		port, err := freeLoopbackPort()
		if err != nil {
			return nil, err
		}
		target = "tcp://127.0.0.1:" + port
	}
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}

	args := []string{"daemon", "--target", target}
	args = append(args, daemonArgs(opts)...)
	cmd := exec.Command(executable, args...)
	setDetachedProcessAttr(cmd)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &Process{PID: cmd.Process.Pid, Target: target, cmd: cmd}, nil
}

// Kill terminates the daemon and reaps it; calling it twice is harmless.
func (p *Process) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.killed {
		p.killed = true
		if err := killProcess(p.cmd); err != nil {
			return err
		}
	}
	_ = p.reap() // a killed daemon always exits non-zero; the kill itself is what Kill reports
	return nil
}

// Wait blocks until the daemon exits and reports its exit status; it reaps at most once.
func (p *Process) Wait() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reap()
}

// reap waits for the child exactly once; the caller must hold p.mu.
func (p *Process) reap() error {
	if p.waited {
		return p.waitErr
	}
	p.waited = true
	p.waitErr = p.cmd.Wait()
	return p.waitErr
}

// Alive reports whether the daemon is still running.
func (p *Process) Alive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.waited && p.cmd.ProcessState == nil
}

// daemonArgs renders the session flags a spawned daemon inherits, omitting everything left at its zero value.
func daemonArgs(opts SpawnOptions) []string {
	var args []string
	flags := []struct {
		name  string
		value string
	}{
		{"--data-dir", opts.DataDir},
		{"--tls-cert", opts.TLSCert},
		{"--tls-key", opts.TLSKey},
		{"--auth-method", opts.AuthMethod},
		{"--auth-token-file", opts.AuthTokenFile},
		{"--tls-client-ca", opts.TLSClientCA},
	}
	for _, f := range flags {
		if f.value != "" {
			args = append(args, f.name, f.value)
		}
	}
	if opts.TLSEnabled {
		args = append(args, "--tls-enabled")
	}
	return append(args, opts.ExtraArgs...)
}

// freeLoopbackPort asks the kernel for an unused port and hands it straight back.
func freeLoopbackPort() (string, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	defer ln.Close()
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return "", fmt.Errorf("free port %s: %w", ln.Addr(), err)
	}
	return port, nil
}

// WaitForAlive polls target until a daemon answers or the deadline passes.
func WaitForAlive(target string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for {
		if IsDaemonAlive(target) {
			return true
		}
		if !time.Now().Before(deadline) {
			return false
		}
		time.Sleep(50 * time.Millisecond)
	}
}
