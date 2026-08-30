package mikyu_test

import (
	"context"
	"testing"

	"github.com/dmunish/mikyu"
)

func TestConsumerGroup_MarkAndCommit(t *testing.T) {
	addr := startServer(t)
	c, err := mikyu.Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	if err := c.CreateQueue(ctx, "group-test"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	cg := c.JoinGroup("group-test", "g1")
	defer cg.Close()

	if _, err := c.Produce(ctx, "group-test", []byte("hello")); err != nil {
		t.Fatalf("Produce: %v", err)
	}

	cg.Mark(0)
	if err := cg.Commit(ctx); err != nil {
		t.Errorf("Commit: %v", err)
	}

	fetched, err := c.FetchOffset(ctx, "group-test", "g1")
	if err != nil {
		t.Errorf("FetchOffset: %v", err)
	}
	if fetched != 0 {
		t.Errorf("fetched offset = %d, want 0", fetched)
	}
}
