package bench

import (
	"context"
	"fmt"

	"github.com/dmunish/mikyu"
)

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
		client, err := mikyu.Open(opts.Dir)
		if err != nil {
			return nil, err
		}
		return &accessHandle{Client: client, Cleanup: client.Close}, nil
	case AccessDaemon, AccessRemote:
		return nil, fmt.Errorf("%w: %s access", errUnimplemented, opts.Access)
	default:
		return nil, fmt.Errorf("%w: unknown access %q", ErrUsage, opts.Access)
	}
}
