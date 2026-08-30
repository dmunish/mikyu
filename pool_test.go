package mikyu_test

import (
	"context"
	"sync"
	"testing"

	"github.com/dmunish/mikyu"
)

func TestPool_ConcurrentProduce(t *testing.T) {
	addr := startServer(t)
	c, err := mikyu.Dial(addr, mikyu.WithMaxConns(5))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	ctx := context.Background()
	if err := c.CreateQueue(ctx, "pool-test"); err != nil {
		t.Fatalf("CreateQueue: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := c.Produce(ctx, "pool-test", []byte("msg"))
			if err != nil {
				t.Errorf("Produce #%d: %v", n, err)
			}
		}(i)
	}
	wg.Wait()

	tail, err := c.LastOffset(ctx, "pool-test")
	if err != nil {
		t.Fatalf("LastOffset: %v", err)
	}
	if tail != 20 {
		t.Errorf("tail = %d, want 20", tail)
	}
}

func TestPool_CloseDrainsConnections(t *testing.T) {
	addr := startServer(t)
	c, err := mikyu.Dial(addr, mikyu.WithMaxConns(3))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Errorf("Close: %v", err)
	}
}
