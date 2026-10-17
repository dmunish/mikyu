package daemon

import (
	"io"
	"os"
	"testing"

	"go.uber.org/goleak"
)

// daemonHelperEnv re-executes the test binary as a real daemon, so
// Spawn's detached-daemon path can be exercised without the built CLI.
const daemonHelperEnv = "MIKYU_DAEMON_HELPER"

func TestMain(m *testing.M) {
	if os.Getenv(daemonHelperEnv) == "1" {
		// Spawned as: <test-binary> daemon --target ... [--data-dir ...].
		if err := Run(os.Args[2:], io.Discard); err != nil {
			os.Exit(1)
		}
		return
	}
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*iocpPort).loop"),
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*ring).loop"),
	)
}
