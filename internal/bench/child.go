package bench

import (
	"context"
	"flag"
	"fmt"

	"github.com/dmunish/mikyu"
)

// childEnvName marks the hidden produce child that recovery mode spawns and kills.
const childEnvName = "MIKYU_BENCH_CHILD"

// RunChild runs the hidden produce child that recovery mode spawns to be killed mid-write.
func RunChild(args []string) error {
	fs := flag.NewFlagSet("bench-child", flag.ContinueOnError)
	dir := fs.String("dir", "", "data directory to write into")
	kind := fs.String("kind", KindLog, "queue kind: log or task")
	queue := fs.String("queue", "", "queue name to write into")
	msgSize := fs.Int("msg-size", 256, "payload size in bytes")
	seed := fs.Uint("seed", 0, "PRNG seed")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *dir == "" || *queue == "" || *msgSize < 1 {
		return fmt.Errorf("%w: bench child needs --dir and --queue", ErrUsage)
	}

	client, err := mikyu.Open(*dir)
	if err != nil {
		return err
	}
	defer client.Close()
	return childProduce(context.Background(), client, *kind, *queue, *msgSize, uint32(*seed))
}

// childProduce appends seeded payloads until the parent kills it.
func childProduce(ctx context.Context, client mikyu.Client, kind, queue string, msgSize int, seed uint32) error {
	buf := make([]byte, msgSize)
	var offset uint64
	if kind == KindTask {
		taskQueue, err := client.TaskQueue(queue)
		if err != nil {
			return err
		}
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			payloadFor(seed, offset, buf)
			if _, err := taskQueue.Enqueue(ctx, buf); err != nil {
				return err
			}
			offset++
		}
	}
	logQueue, err := client.LogQueue(queue)
	if err != nil {
		return err
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		payloadFor(seed, offset, buf)
		if _, err := logQueue.Append(ctx, buf); err != nil {
			return err
		}
		offset++
	}
}
