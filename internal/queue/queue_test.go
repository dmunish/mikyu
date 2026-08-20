package queue

import (
	"bytes"
	"errors"
	"testing"
)

func TestManager_NewQueue(t *testing.T) {
	t.Run("creates a queue", func(t *testing.T) {
		m := NewManager()
		q, err := m.NewQueue("orders", 1024)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if q == nil {
			t.Fatal("expected non-nil queue")
		}
	})

	t.Run("duplicate name returns ErrQueueExists", func(t *testing.T) {
		m := NewManager()
		if _, err := m.NewQueue("dup", 1024); err != nil {
			t.Fatalf("first create failed: %v", err)
		}
		_, err := m.NewQueue("dup", 1024)
		if !errors.Is(err, ErrQueueExists) {
			t.Fatalf("expected ErrQueueExists, got %v", err)
		}
	})
}

func TestManager_GetQueue(t *testing.T) {
	t.Run("returns existing queue", func(t *testing.T) {
		m := NewManager()
		_, _ = m.NewQueue("orders", 1024)
		if _, err := m.GetQueue("orders"); err != nil {
			t.Fatalf("expected found, got %v", err)
		}
	})

	t.Run("missing name returns ErrQueueNotFound", func(t *testing.T) {
		_, err := NewManager().GetQueue("nope")
		if !errors.Is(err, ErrQueueNotFound) {
			t.Fatalf("expected ErrQueueNotFound, got %v", err)
		}
	})
}

func TestQueue_PushPop(t *testing.T) {
	t.Run("preserves FIFO order", func(t *testing.T) {
		q, _ := NewManager().NewQueue("q", 1024)

		payload1 := []byte{0x00, 0x01, 0x02, 0xFF, 0xFE}
		payload2 := []byte{0xDE, 0xAD, 0xBE, 0xEF}

		id1, _ := q.Push(payload1)
		id2, _ := q.Push(payload2)
		if id1 == "" || id2 == "" {
			t.Fatal("expected non-empty message IDs")
		}

		popped1, err := q.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(popped1.Payload, payload1) {
			t.Fatalf("pop 1 = %v, want %v", popped1.Payload, payload1)
		}

		popped2, err := q.Pop()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if !bytes.Equal(popped2.Payload, payload2) {
			t.Fatalf("pop 2 = %v, want %v", popped2.Payload, payload2)
		}
	})

	t.Run("pop on empty returns ErrQueueEmpty", func(t *testing.T) {
		empty, _ := NewManager().NewQueue("empty", 1024)
		if _, err := empty.Pop(); !errors.Is(err, ErrQueueEmpty) {
			t.Fatalf("expected ErrQueueEmpty, got %v", err)
		}
	})
}
