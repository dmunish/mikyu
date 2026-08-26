package mikyu_test

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/queue"
)

// startServer opens a Manager in a temp dir and serves it on an ephemeral
// localhost port, returning the address to dial.
func startServer(t *testing.T) string {
	t.Helper()

	dir := t.TempDir()
	mgr, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("queue.Open: %v", err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go server.New(mgr).Serve(ln)

	t.Cleanup(func() {
		ln.Close()
		mgr.Close()
	})
	return ln.Addr().String()
}

func dialClient(t *testing.T, addr string) *mikyu.Client {
	t.Helper()

	c, err := mikyu.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestClientEndToEnd_ProduceReadTailLifecycle(t *testing.T) {
	ctx := context.Background()
	addr := startServer(t)
	c := dialClient(t, addr)

	if err := c.CreateQueue(ctx, "orders"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if err := c.CreateQueue(ctx, "orders"); !errors.Is(err, mikyu.ErrQueueExists) {
		t.Errorf("duplicate CreateQueue err = %v, want %v", err, mikyu.ErrQueueExists)
	}

	payloads := [][]byte{[]byte("alpha"), []byte("beta"), []byte("gamma")}
	var ids []string
	for i, p := range payloads {
		offset, err := c.Produce(ctx, "orders", p)
		if err != nil {
			t.Fatalf("Produce #%d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("Produce #%d offset = %d, want %d", i, offset, i)
		}
		msg, err := c.Read(ctx, "orders", offset)
		if err != nil {
			t.Fatalf("Read #%d: %v", i, err)
		}
		if msg.ID == "" {
			t.Errorf("Read #%d returned empty ID", i)
		}
		if !bytes.Equal(msg.Payload, p) || msg.Offset != uint64(i) {
			t.Errorf("Read #%d = {offset:%d payload:%q}, want {offset:%d payload:%q}",
				i, msg.Offset, msg.Payload, i, p)
		}
		ids = append(ids, msg.ID)
	}

	// IDs are unique per message.
	for i := 0; i < len(ids); i++ {
		for j := i + 1; j < len(ids); j++ {
			if ids[i] == ids[j] {
				t.Errorf("duplicate message ID across offsets %d and %d: %q", i, j, ids[i])
			}
		}
	}

	tail, err := c.LastOffset(ctx, "orders")
	if err != nil {
		t.Fatalf("LastOffset: %v", err)
	}
	if tail != uint64(len(payloads)) {
		t.Errorf("LastOffset = %d, want %d", tail, len(payloads))
	}
}

func TestClientEndToEnd_ErrorMapping(t *testing.T) {
	ctx := context.Background()
	addr := startServer(t)
	c := dialClient(t, addr)

	if _, err := c.Produce(ctx, "missing", []byte("x")); !errors.Is(err, mikyu.ErrQueueNotFound) {
		t.Errorf("produce-to-missing err = %v, want %v", err, mikyu.ErrQueueNotFound)
	}
	if _, err := c.Read(ctx, "missing", 0); !errors.Is(err, mikyu.ErrQueueNotFound) {
		t.Errorf("read-from-missing err = %v, want %v", err, mikyu.ErrQueueNotFound)
	}
	if _, err := c.LastOffset(ctx, "missing"); !errors.Is(err, mikyu.ErrQueueNotFound) {
		t.Errorf("tail-of-missing err = %v, want %v", err, mikyu.ErrQueueNotFound)
	}

	// Queue exists but offset is past the tail.
	if err := c.CreateQueue(ctx, "empty"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}
	if _, err := c.Read(ctx, "empty", 0); !errors.Is(err, mikyu.ErrOffsetNotFound) {
		t.Errorf("read-past-tail err = %v, want %v", err, mikyu.ErrOffsetNotFound)
	}
}

func TestClientEndToEnd_ContextDeadlineHonored(t *testing.T) {
	addr := startServer(t)
	c := dialClient(t, addr)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// The queue doesn't exist; the request should fail either from the server's
	// error response or the deadline — never hang.
	done := make(chan error, 1)
	go func() {
		_, err := c.Produce(ctx, "slow", []byte("x"))
		done <- err
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Produce with expired deadline did not return within 2s")
	}
}

func TestClientEndToEnd_MultipleQueuesAreIsolated(t *testing.T) {
	ctx := context.Background()
	addr := startServer(t)
	c := dialClient(t, addr)

	for _, name := range []string{"a", "b"} {
		if err := c.CreateQueue(ctx, name); err != nil {
			t.Fatalf("CreateQueue(%q): %v", name, err)
		}
	}

	if _, err := c.Produce(ctx, "a", []byte("for-a")); err != nil {
		t.Fatalf("Produce a: %v", err)
	}

	msg, err := c.Read(ctx, "b", 0)
	if !errors.Is(err, mikyu.ErrOffsetNotFound) {
		t.Fatalf("read empty queue b err = %v, want %v", err, mikyu.ErrOffsetNotFound)
	}
	_ = msg

	tailA, err := c.LastOffset(ctx, "a")
	if err != nil || tailA != 1 {
		t.Errorf("queue a LastOffset = %d, %v; want 1, <nil>", tailA, err)
	}
	tailB, err := c.LastOffset(ctx, "b")
	if err != nil || tailB != 0 {
		t.Errorf("queue b LastOffset = %d, %v; want 0, <nil>", tailB, err)
	}
}
