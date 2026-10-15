package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
)

// daemonReadyTimeout bounds how long a run waits for its spawned daemon to accept connections.
const daemonReadyTimeout = 5 * time.Second

// accessHandle carries the client one run opened and how to tear it down.
type accessHandle struct {
	Client    mikyu.Client
	Cleanup   func() error
	Target    string
	DaemonPID int
}

// openClient opens the client for the run's access mode, clearing any stale daemon first.
func openClient(_ context.Context, opts Options) (*accessHandle, error) {
	switch opts.Access {
	case AccessEmbedded:
		if err := killStaleDaemons(opts.Dir, nil); err != nil {
			return nil, err
		}
		client, err := mikyu.Open(opts.Dir, mikyu.WithEncryptionKeyFile(opts.KeyFile))
		if err != nil {
			return nil, err
		}
		return &accessHandle{Client: client, Cleanup: client.Close}, nil
	case AccessDaemon:
		return openDaemon(opts)
	case AccessRemote:
		client, err := mikyu.Dial(opts.Target)
		if err != nil {
			return nil, err
		}
		return &accessHandle{Client: client, Cleanup: client.Close, Target: opts.Target}, nil
	default:
		return nil, fmt.Errorf("%w: unknown access %q", ErrUsage, opts.Access)
	}
}

// openDaemon spawns a daemon this run owns and dials it.
func openDaemon(opts Options) (*accessHandle, error) {
	if err := killStaleDaemons(opts.Dir, nil); err != nil {
		return nil, err
	}
	proc, err := daemon.Spawn(daemon.SpawnOptions{DataDir: opts.Dir, EncryptionKeyFile: opts.KeyFile})
	if err != nil {
		return nil, err
	}
	if !daemon.WaitForAlive(proc.Target, daemonReadyTimeout) {
		_ = proc.Kill()
		return nil, fmt.Errorf("%w within %s", errDaemonStart, daemonReadyTimeout)
	}
	client, err := mikyu.Dial(proc.Target)
	if err != nil {
		_ = proc.Kill()
		return nil, err
	}
	cleanup := func() error {
		_ = client.Close()
		if opts.Persist {
			return nil
		}
		return proc.Kill()
	}
	return &accessHandle{Client: client, Cleanup: cleanup, Target: proc.Target, DaemonPID: proc.PID}, nil
}
