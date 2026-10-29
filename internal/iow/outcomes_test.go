package iow

import (
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

// A waited tag is how Sync observes its lone fsync: the outcome must survive until
// wait consumes it instead of being erased by the completing reaper.
func TestWaitedTagReportsItsError(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(7, 1, nil))
	o.complete(7, -int64(syscall.EIO))
	require.ErrorIs(t, o.wait(7), syscall.EIO)
}

func TestWaitedTagStaysCleanOnSuccess(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(7, 1, nil))
	o.complete(7, 16) // bytes written
	require.NoError(t, o.wait(7))
}

func TestWaitedTagWaitsForEveryCompletionAndKeepsTheFirstError(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(9, 3, nil))

	done := make(chan error, 1)
	go func() { done <- o.wait(9) }()

	o.complete(9, -int64(syscall.ENOSPC))
	o.complete(9, 0)
	select {
	case err := <-done:
		t.Fatalf("wait returned before the last completion: %v", err)
	default:
	}
	o.complete(9, -int64(syscall.EIO))
	require.ErrorIs(t, <-done, syscall.ENOSPC)
}

func TestTagCallbackFiresOnceWithTheFirstError(t *testing.T) {
	o := newOutcomes()
	var mu sync.Mutex
	var calls int
	var got error
	require.NoError(t, o.register(3, 2, func(err error) {
		mu.Lock()
		calls++
		got = err
		mu.Unlock()
	}))

	o.complete(3, -int64(syscall.ENOSPC))
	mu.Lock()
	require.Zero(t, calls, "done must wait for the tag's last completion")
	mu.Unlock()

	o.complete(3, -int64(syscall.EIO))
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, calls)
	require.ErrorIs(t, got, syscall.ENOSPC)
}

func TestCompletionAfterWaitIsIgnored(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(4, 1, nil))
	o.complete(4, 0)
	require.NoError(t, o.wait(4))

	o.complete(4, -int64(syscall.EIO)) // must not resurrect the tag
	require.NoError(t, o.wait(4))
}

func TestClosedOutcomesRejectRegistrationAndWakeWaiters(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(1, 2, nil))
	errc := make(chan error, 1)
	go func() { errc <- o.wait(1) }()

	require.True(t, o.close())
	require.ErrorIs(t, <-errc, ErrClosed)
	require.False(t, o.close())
	require.ErrorIs(t, o.register(2, 1, nil), ErrClosed)
}

func TestRolledBackTagLeavesNoOutcome(t *testing.T) {
	o := newOutcomes()
	require.NoError(t, o.register(5, 2, func(error) { t.Error("rolled-back tag must not fire done") }))
	o.rollback(5, 2)
	o.complete(5, -int64(syscall.EIO))

	require.NoError(t, o.wait(5))
	require.Empty(t, o.pend)
	require.Empty(t, o.res)
	require.Empty(t, o.dones)
}