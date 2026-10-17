package repl

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
)

const daemonStartTimeout = 10 * time.Second

// handleOpen attaches the session to a data directory in-process, the way the
// SDK's mikyu.Open does: no daemon, the engine lives in this process.
func (s *replSession) handleOpen(args []string) {
	if len(args) != 1 {
		s.usage("open <data-dir>")
		return
	}
	dir := args[0]
	sess := s.session
	sess.DataDir = dir
	c, err := command.OpenClient(sess)
	if err != nil {
		s.fail(err)
		return
	}
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.conn = c
	s.dir = dir
	s.activeQueue = ""
	s.activeKind = 0
	fmt.Println(successStyle.Render(fmt.Sprintf("● Opened %q in-process (embedded).", dir)))
}

// handleClose detaches from the current client; the next command reconnects.
func (s *replSession) handleClose(_ []string) {
	if s.conn == nil {
		fmt.Println(dimStyle.Render("Not connected."))
		return
	}
	if err := s.conn.Close(); err != nil {
		s.fail(err)
		return
	}
	s.conn = nil
	s.activeQueue = ""
	s.activeKind = 0
	fmt.Println(infoStyle.Render("Closed the connection."))
}

// handleDaemon manages the daemon behind the session's target: start spawns a
// detached process; status, stop, and logs reuse the daemon client
// subcommands with this session's target and credential.
func (s *replSession) handleDaemon(args []string) {
	sub := "status"
	if len(args) > 0 {
		sub, args = args[0], args[1:]
	}
	switch sub {
	case "start":
		s.handleDaemonStart(args)
	case "status", "stop", "logs":
		sess := s.session
		if err := spec.ApplyEnvSession(&sess, os.Getenv); err != nil {
			s.fail(err)
			return
		}
		if err := daemon.RunClientSub(sub, args, sess, os.Stdout, os.Stderr); err != nil {
			if errors.Is(err, daemon.ErrUsage) {
				s.usage("daemon start | status | stop | logs")
				return
			}
			s.fail(err)
		}
	case "run":
		s.usage("daemon run blocks the terminal; use: daemon start")
	default:
		s.usage("daemon start | status | stop | logs")
	}
}

// defaultDaemonDir is where `daemon start` stores data when --dir is omitted:
// the directory the REPL was launched from, falling back to the executable's
// directory and then the current directory when the OS will not say.
func defaultDaemonDir() string {
	if wd, err := os.Getwd(); err == nil {
		return wd
	}
	if exe, err := os.Executable(); err == nil {
		return filepath.Dir(exe)
	}
	return "."
}

// handleDaemonStart launches a detached daemon for the session's target and
// waits for it to accept connections. --dir defaults to the directory the
// REPL was launched from; other flags pass through to the daemon.
func (s *replSession) handleDaemonStart(args []string) {
	var dir string
	fs := command.NewFlagSet("daemon start")
	fs.StringVar(&dir, "dir", defaultDaemonDir(), "data directory for the daemon (default: the current directory)")
	rest, err := command.ParseAnywhere(fs, args)
	if err != nil {
		s.usage("daemon start [--dir <data-dir>] [daemon flags...] ("+err.Error()+")")
		return
	}
	if dir == "" {
		s.usage("daemon start [--dir <data-dir>] [daemon flags...]")
		return
	}
	if daemon.IsDaemonAlive(s.target) {
		fmt.Println(infoStyle.Render(fmt.Sprintf("A daemon is already serving %s.", s.target)))
		return
	}

	opts := daemon.SpawnOptions{
		Target:            s.target,
		DataDir:           dir,
		TLSEnabled:        s.session.TLS.Enabled,
		TLSCert:           s.session.TLS.Cert,
		TLSKey:            s.session.TLS.Key,
		TLSClientCA:       s.session.TLS.ClientCA,
		AuthMethod:        s.session.Auth.Method,
		AuthTokenFile:     s.session.Auth.TokenFile,
		EncryptionKeyFile: s.session.Encryption.KeyFile,
		ExtraArgs:         rest,
	}
	proc, err := daemon.Spawn(opts)
	if err != nil {
		s.fail(err)
		return
	}
	if !daemon.WaitForAlive(proc.Target, daemonStartTimeout) {
		s.fail(fmt.Errorf("daemon pid %d did not accept connections on %s within %s; check its output above",
			proc.PID, proc.Target, daemonStartTimeout))
		return
	}
	fmt.Println(successStyle.Render(fmt.Sprintf("● daemon serving %s (pid %d, data dir %s)", proc.Target, proc.PID, dir)))
}
