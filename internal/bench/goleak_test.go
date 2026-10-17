package bench

import (
	"io"
	"os"
	"testing"

	"github.com/dmunish/mikyu/internal/daemon"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	if os.Getenv(childEnvName) == "1" {
		if err := RunChild(os.Args[2:]); err != nil {
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "daemon" {
		if err := daemon.Run(os.Args[2:], io.Discard); err != nil {
			os.Exit(1)
		}
		return
	}
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*iocpPort).loop"),
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*ring).loop"),
	)
}
