package logqueue_test

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*iocpPort).loop"),
		goleak.IgnoreAnyFunction("github.com/dmunish/mikyu/internal/iow.(*ring).loop"),
	)
}
