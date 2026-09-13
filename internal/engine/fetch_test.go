package engine

import (
	"context"
	"testing"
)

func TestFetchReturnsAvailable(t *testing.T) {
	q, _ := newTestQueue(t, 256)
	defer q.Close()
	if _, err := q.Append(context.TODO(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	msgs, err := q.Fetch(context.TODO(), 0, 10, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1, got %d", len(msgs))
	}
}

func TestFetchHeadIsNonBlocking(t *testing.T) {
	q, _ := newTestQueue(t, 256)
	defer q.Close()
	_, err := q.Fetch(context.TODO(), 0, 10, 0)
	if err != ErrNoData {
		t.Fatalf("expected ErrNoData, got %v", err)
	}
}
