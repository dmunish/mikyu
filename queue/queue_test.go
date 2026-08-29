package queue_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/queue"
)

// openManager opens a Manager in a fresh temp dir and registers cleanup.
func openManager(t *testing.T) *queue.Manager {
	t.Helper()
	m, err := queue.Open(t.TempDir())
	if err != nil {
		t.Fatalf("queue.Open: %v", err)
	}
	// Windows locks open files; TempDir cleanup fails unless the WALs are closed first.
	t.Cleanup(func() { m.Close() })
	return m
}

func mustCreateQueue(t *testing.T, m *queue.Manager, name string) *queue.Queue {
	t.Helper()
	q, err := m.CreateQueue(name)
	if err != nil {
		t.Fatalf("CreateQueue(%q): %v", name, err)
	}
	return q
}

func TestManager_CreateAndGet(t *testing.T) {
	t.Parallel()

	m := openManager(t)

	q := mustCreateQueue(t, m, "orders")
	if got := q; got == nil {
		t.Fatal("CreateQueue returned nil queue without error")
	}

	_, err := m.CreateQueue("orders")
	if !errors.Is(err, queue.ErrQueueExists) {
		t.Errorf("duplicate create err = %v, want %v", err, queue.ErrQueueExists)
	}

	got, err := m.Queue("orders")
	if err != nil {
		t.Fatalf("Queue(%q): %v", "orders", err)
	}
	if got != q {
		t.Error("Queue returned a different instance than CreateQueue")
	}

	if _, err := m.Queue("missing"); !errors.Is(err, queue.ErrQueueNotFound) {
		t.Errorf("missing queue err = %v, want %v", err, queue.ErrQueueNotFound)
	}
}

func TestManager_RestoreQueuesFromDisk(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	m, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	mustCreateQueue(t, m, "events")
	mustCreateQueue(t, m, "billing")
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	for _, name := range []string{"events", "billing"} {
		if _, err := reopened.Queue(name); err != nil {
			t.Errorf("restored %q: %v", name, err)
		}
	}
	if _, err := reopened.CreateQueue("events"); !errors.Is(err, queue.ErrQueueExists) {
		t.Errorf("re-created restored queue: err = %v, want %v", err, queue.ErrQueueExists)
	}
}

func TestProduceAndRead(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	m := openManager(t)
	q := mustCreateQueue(t, m, "orders")

	payloads := [][]byte{[]byte("first"), []byte("second"), []byte("third")}
	for i, p := range payloads {
		offset, err := q.Produce(ctx, p)
		if err != nil {
			t.Fatalf("Produce #%d: %v", i, err)
		}
		if offset != uint64(i) {
			t.Errorf("Produce #%d offset = %d, want %d", i, offset, i)
		}
	}

	if got, want := q.LastOffset(), uint64(len(payloads)); got != want {
		t.Errorf("LastOffset = %d, want %d", got, want)
	}

	for i, want := range payloads {
		msg, err := q.Read(ctx, uint64(i))
		if err != nil {
			t.Fatalf("Read(%d): %v", i, err)
		}
		if msg.Offset != uint64(i) {
			t.Errorf("Read(%d) msg.Offset = %d", i, msg.Offset)
		}
		if msg.ID == "" {
			t.Errorf("Read(%d) returned empty ID", i)
		}
		if msg.Timestamp.IsZero() {
			t.Errorf("Read(%d) returned zero Timestamp", i)
		}
		if !bytes.Equal(msg.Payload, want) {
			t.Errorf("Read(%d) payload = %q, want %q", i, msg.Payload, want)
		}
	}

	if _, err := q.Read(ctx, uint64(len(payloads))); !errors.Is(err, queue.ErrOffsetNotFound) {
		t.Errorf("read past tail err = %v, want %v", err, queue.ErrOffsetNotFound)
	}
}

func TestWALReplay_PreservesMessagesAcrossReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dir := t.TempDir()

	m, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	q := mustCreateQueue(t, m, "durable")

	type written struct {
		id      string
		payload []byte
	}
	var want []written
	for i := 0; i < 5; i++ {
		p := []byte{byte(i), byte(i), byte(i)}
		if _, err := q.Produce(ctx, p); err != nil {
			t.Fatalf("Produce #%d: %v", i, err)
		}
		msg, err := q.Read(ctx, uint64(i))
		if err != nil {
			t.Fatalf("Read #%d: %v", i, err)
		}
		want = append(want, written{id: msg.ID, payload: msg.Payload})
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	rq, err := reopened.Queue("durable")
	if err != nil {
		t.Fatalf("reopened Queue: %v", err)
	}
	if got := rq.LastOffset(); got != uint64(len(want)) {
		t.Fatalf("after replay LastOffset = %d, want %d", got, len(want))
	}
	for i, w := range want {
		msg, err := rq.Read(ctx, uint64(i))
		if err != nil {
			t.Fatalf("replayed Read(%d): %v", i, err)
		}
		if msg.ID != w.id || !bytes.Equal(msg.Payload, w.payload) {
			t.Errorf("replayed Read(%d) = {id:%q payload:%q}, want {id:%q payload:%q}",
				i, msg.ID, msg.Payload, w.id, w.payload)
		}
	}
}

func TestWALReplay_TruncatesTornWriteTail(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	m, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	const good = 3
	q := mustCreateQueue(t, m, "torn")
	for i := 0; i < good; i++ {
		if _, err := q.Produce(context.Background(), []byte("valid")); err != nil {
			t.Fatalf("Produce #%d: %v", i, err)
		}
	}
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-write: append a length prefix claiming more bytes
	// than actually follow, plus a stray trailing byte with no complete header.
	walPath := filepath.Join(dir, "torn", "00000000000000000000.log")
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("open wal for corruption: %v", err)
	}
	var tornHeader [4]byte
	binary.BigEndian.PutUint32(tornHeader[:], 1024) // body claims 1024 bytes...
	if _, err := f.Write(tornHeader[:]); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	if _, err := f.Write([]byte{0xDE}); err != nil { // ...but only 1 arrives
		t.Fatalf("write torn body byte: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close wal: %v", err)
	}

	reopened, err := queue.Open(dir)
	if err != nil {
		t.Fatalf("reopen after torn write: %v (want clean truncation)", err)
	}
	defer reopened.Close()

	rq, err := reopened.Queue("torn")
	if err != nil {
		t.Fatalf("reopened Queue: %v", err)
	}
	if got := rq.LastOffset(); got != good {
		t.Errorf("after truncation LastOffset = %d, want %d", got, good)
	}
	msg, err := rq.Read(context.Background(), good-1)
	if err != nil {
		t.Fatalf("Read last valid message: %v", err)
	}
	if string(msg.Payload) != "valid" {
		t.Errorf("last valid payload = %q, want %q", msg.Payload, "valid")
	}
}
