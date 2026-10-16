package mikyu_test

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/dmunish/mikyu"
)

func ExampleOpen() {
	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	events, err := client.OpenLog(ctx, "events")
	if err != nil {
		log.Fatal(err)
	}

	offset, err := events.Append(ctx, []byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	record, err := events.Read(ctx, offset)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("appended at", offset, "and read back:", string(record.Payload))

	// Output: appended at 0 and read back: hello
}

func ExampleDial() {
	client, err := mikyu.Dial("tcp://127.0.0.1:7420")
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	queues, err := client.ListQueues(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, q := range queues {
		fmt.Println(q.Kind, q.Name)
	}
}

func ExampleClient_OpenLog() {
	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	first, err := client.OpenLog(ctx, "events", mikyu.WithMaxSegment(1<<20))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := first.Append(ctx, []byte("first")); err != nil {
		log.Fatal(err)
	}

	second, err := client.OpenLog(ctx, "events")
	if err != nil {
		log.Fatal(err)
	}
	last, err := second.LastOffset(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("reopened the same queue, next offset is", last)

	// Output: reopened the same queue, next offset is 1
}

func ExampleClient_OpenTask() {
	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	jobs, err := client.OpenTask(ctx, "emails")
	if err != nil {
		log.Fatal(err)
	}

	if _, err := jobs.Enqueue(ctx, []byte("send the welcome email")); err != nil {
		log.Fatal(err)
	}

	task, err := jobs.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("picked up task on attempt", task.Attempts)

	if err := jobs.Ack(ctx, task); err != nil {
		log.Fatal(err)
	}
	fmt.Println("done")

	// Output:
	// picked up task on attempt 1
	// done
}

func ExampleLogQueue_Group() {
	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	events, err := client.OpenLog(ctx, "events")
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := events.Append(ctx, []byte(fmt.Sprintf("event-%d", i))); err != nil {
			log.Fatal(err)
		}
	}

	group := events.Group("workers")
	for offset := uint64(0); offset < 3; offset++ {
		record, err := events.Read(ctx, offset)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("processed", string(record.Payload))
		group.Mark(offset)
	}
	if err := group.Commit(ctx); err != nil {
		log.Fatal(err)
	}
	position, err := group.Committed(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("group position:", position)

	// Output:
	// processed event-0
	// processed event-1
	// processed event-2
	// group position: 3
}

func ExampleNewTyped() {
	type order struct {
		ID  int    `json:"id"`
		SKU string `json:"sku"`
	}

	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	events, err := client.OpenLog(ctx, "orders")
	if err != nil {
		log.Fatal(err)
	}
	orders := mikyu.NewTyped(events, mikyu.JSONCodec[order]())

	offset, err := orders.Append(ctx, order{ID: 1, SKU: "widget"})
	if err != nil {
		log.Fatal(err)
	}
	got, err := orders.Read(ctx, offset)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("order", got.ID, "is a", got.SKU)

	// Output: order 1 is a widget
}

func ExampleNewTypedTask() {
	type job struct {
		Target string `json:"target"`
	}

	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	ctx := context.Background()
	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	queue, err := client.OpenTask(ctx, "notifications")
	if err != nil {
		log.Fatal(err)
	}
	notifications := mikyu.NewTypedTask(queue, mikyu.JSONCodec[job]())

	if _, err := notifications.Enqueue(ctx, job{Target: "ops@example.com"}); err != nil {
		log.Fatal(err)
	}

	delivered, err := notifications.Dequeue(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("notify", delivered.Value.Target)

	if err := notifications.Ack(ctx, delivered); err != nil {
		log.Fatal(err)
	}

	// Output: notify ops@example.com
}

func ExampleIsNotFound() {
	dir, err := os.MkdirTemp("", "mikyu-example")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	client, err := mikyu.Open(dir)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	_, err = client.Log(context.Background(), "metrics")
	fmt.Println("a missing queue:", mikyu.IsNotFound(err))

	// Output: a missing queue: true
}
