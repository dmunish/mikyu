package mikyu_test

import (
	"context"
	"testing"

	"github.com/dmunish/mikyu"
)

func TestStream_EndsAtTail(t *testing.T) {
	addr := startServer(t)
	c, err := mikyu.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	if err := c.CreateQueue(ctx, "stream-test"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	count := 0
	for msg, err := range c.Stream(ctx, "stream-test", 0) {
		if err != nil {
			t.Fatalf("Stream err: %v", err)
		}
		_ = msg
		count++
		if count > 5 {
			t.Fatal("Stream did not end at tail")
		}
	}
	if count != 0 {
		t.Errorf("expected 0 messages (empty queue), got %d", count)
	}
}
