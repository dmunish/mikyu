package command

import (
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
)

// Shutdown sends an OpShutdown frame to the daemon, gated by confirmFn when provided.
func Shutdown(target string, sess spec.Session, confirmFn func() (bool, error)) (executed bool, err error) {
	if confirmFn != nil {
		ok, err := confirmFn()
		if err != nil || !ok {
			return false, err
		}
	}
	if err := daemon.SendShutdownSignal(target, sess); err != nil {
		return true, err
	}
	return true, nil
}
