package queue

import (
	"errors"
	"testing"
)

func TestManager_NewQueue(t *testing.T) {
	t.Run("creates a queue", func(t *testing.T) {
		m := NewManager()
		q, err := m.NewQueue("orders")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if q == nil {
			t.Fatal("expected non-nil queue")
		}
	})

	t.Run("duplicate name returns ErrQueueExists", func(t *testing.T) {
		m := NewManager()
		if _, err := m.NewQueue("dup"); err != nil {
			t.Fatalf("first create failed: %v", err)
		}
		_, err := m.NewQueue("dup")
		if !errors.Is(err, ErrQueueExists) {
			t.Fatalf("expected ErrQueueExists, got %v", err)
		}
	})
}

func TestManager_GetQueue(t *testing.T) {
	t.Run("returns existing queue", func(t *testing.T) {
		m := NewManager()
		_, _ = m.NewQueue("orders")
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

func TestQueue_PushPeekPop(t *testing.T) {
	t.Run("preserves FIFO order", func(t *testing.T) {
		q, _ := NewManager().NewQueue("q")

		id1, _ := q.Push("first")
		id2, _ := q.Push("second")
		if id1 == "" || id2 == "" {
			t.Fatal("expected non-empty message IDs")
		}

		peeked, err := q.Peek()
		if err != nil {
			t.Fatalf("peek error: %v", err)
		}
		if peeked.Payload != "first" {
			t.Fatalf("peek = %q, want first", peeked.Payload)
		}

		popped, _ := q.Pop()
		if popped.Payload != "first" {
			t.Fatalf("pop = %q, want first", popped.Payload)
		}
		popped, _ = q.Pop()
		if popped.Payload != "second" {
			t.Fatalf("second pop = %q, want second", popped.Payload)
		}
	})

	t.Run("pop on empty returns ErrQueueEmpty", func(t *testing.T) {
		empty, _ := NewManager().NewQueue("empty")
		if _, err := empty.Pop(); !errors.Is(err, ErrQueueEmpty) {
			t.Fatalf("expected ErrQueueEmpty, got %v", err)
		}
	})
}
