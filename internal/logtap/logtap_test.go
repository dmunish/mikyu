package logtap

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRingReplaysOldestFirstAndEvicts(t *testing.T) {
	tap := New(4)
	for i := range 6 {
		fmt.Fprintf(tap, "line-%d\n", i)
	}
	lines := tap.Snapshot()
	require.Len(t, lines, 4)
	for i, line := range lines {
		assert.Equal(t, fmt.Sprintf("line-%d", i+2), string(line), "the oldest lines are evicted first")
	}
}

func TestWriteBuffersPartialLines(t *testing.T) {
	tap := New(8)
	_, err := tap.Write([]byte("hello "))
	require.NoError(t, err)
	assert.Empty(t, tap.Snapshot(), "an incomplete line is not published")

	_, err = tap.Write([]byte("world\nnext"))
	require.NoError(t, err)
	lines := tap.Snapshot()
	require.Len(t, lines, 1)
	assert.Equal(t, "hello world", string(lines[0]))

	_, err = tap.Write([]byte(" line\n"))
	require.NoError(t, err)
	assert.Len(t, tap.Snapshot(), 2)
}

func TestSubscribeReplaysWithoutGapOrOverlap(t *testing.T) {
	tap := New(64)
	fmt.Fprint(tap, "before-1\nbefore-2\n")

	backlog, ch := tap.Subscribe()
	require.Len(t, backlog, 2)
	assert.Equal(t, "before-1", string(backlog[0]))
	assert.Equal(t, "before-2", string(backlog[1]))

	fmt.Fprint(tap, "live-1\n")
	select {
	case line := <-ch:
		assert.Equal(t, "live-1", string(line))
	case <-time.After(time.Second):
		t.Fatal("the live line never reached the follower")
	}

	// A second subscriber's backlog includes the line the first saw live; the
	// ring is the shared record, the channels only extend it.
	backlog2, _ := tap.Subscribe()
	require.Len(t, backlog2, 3)
	assert.Equal(t, "live-1", string(backlog2[2]))
}

func TestSlowFollowerIsDropped(t *testing.T) {
	tap := New(64)
	_, ch := tap.Subscribe()

	for i := range followerBuffer + 10 {
		fmt.Fprintf(tap, "line-%d\n", i)
	}

	// The buffer holds the first followerBuffer lines, then the tap drops the
	// follower and closes its channel.
	dropped := false
	for range followerBuffer {
		select {
		case _, ok := <-ch:
			require.True(t, ok)
		default:
		}
	}
	select {
	case _, ok := <-ch:
		dropped = !ok
	default:
	}
	assert.True(t, dropped, "the follower is closed once it falls behind")

	// The daemon keeps running and accepts a fresh follower.
	fmt.Fprint(tap, "after-drop\n")
	lines := tap.Snapshot()
	assert.Equal(t, "after-drop", string(lines[len(lines)-1]))
}

func TestUnsubscribe(t *testing.T) {
	tap := New(8)
	_, ch := tap.Subscribe()
	tap.Unsubscribe(ch)
	tap.Unsubscribe(ch) // a missing follower is a no-op

	fmt.Fprint(tap, "unobserved\n")
	select {
	case line := <-ch:
		t.Fatalf("a removed follower still received %q", line)
	default:
	}
}

func TestConcurrentWriteAndSubscribe(t *testing.T) {
	tap := New(1024)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				fmt.Fprintf(tap, "writer-%d line-%d\n", i%4, i)
			}
		}()
	}

	for range 50 {
		_, ch := tap.Subscribe()
		time.Sleep(time.Millisecond)
		tap.Unsubscribe(ch)
	}
	close(stop)
	wg.Wait()
}

func TestNewRejectsNonPositiveCapacity(t *testing.T) {
	tap := New(0)
	assert.Equal(t, DefaultCapacity, len(tap.ring))
}

func TestZeroLengthLinesAreNotStored(t *testing.T) {
	tap := New(8)
	fmt.Fprint(tap, "\n\nline\n")
	assert.Equal(t, []string{"line"}, renderedLines(tap.Snapshot()))
}

func renderedLines(lines [][]byte) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = strings.TrimSuffix(string(l), "\n")
	}
	return out
}
