package client

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func (c *remoteClient) LogQueue(name string) (logqueue.ClientLogQueue, error) {
	return &remoteLogQueue{client: c, name: name}, nil
}

type remoteLogQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteLogQueue) Name() string { return q.name }

func (q *remoteLogQueue) Append(ctx context.Context, payload []byte) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteBytes(payload)
	resp, err := q.client.exchange(ctx, protocol.OpAppend, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU32(uint32(len(payloads)))
	for _, p := range payloads {
		body.WriteBytes(p)
	}
	resp, err := q.client.exchange(ctx, protocol.OpAppendBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	count, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	offsets := make([]uint64, count)
	for i := range count {
		off, err := r.ReadU64()
		if err != nil {
			return nil, err
		}
		offsets[i] = off
	}
	return offsets, nil
}

func (q *remoteLogQueue) Read(ctx context.Context, offset uint64) (engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(offset)
	resp, err := q.client.exchange(ctx, protocol.OpRead, body.Bytes())
	if err != nil {
		return engine.Message{}, err
	}
	r := protocol.NewReader(resp)
	off, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	idBytes, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	tsNano, err := r.ReadU64()
	if err != nil {
		return engine.Message{}, err
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return engine.Message{}, err
	}
	return engine.Message{
		Offset:    off,
		ID:        string(idBytes),
		Timestamp: time.Unix(0, int64(tsNano)),
		Payload:   payload,
	}, nil
}

func (q *remoteLogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	body.WriteU64(startOffset)
	body.WriteU32(uint32(count))
	resp, err := q.client.exchange(ctx, protocol.OpReadBatch, body.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	n, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	msgs := make([]engine.Message, 0, n)
	for range n {
		off, _ := r.ReadU64()
		idBytes, _ := r.ReadBytes()
		tsNano, _ := r.ReadU64()
		payload, _ := r.ReadBytes()
		msgs = append(msgs, engine.Message{
			Offset:    off,
			ID:        string(idBytes),
			Timestamp: time.Unix(0, int64(tsNano)),
			Payload:   payload,
		})
	}
	return msgs, nil
}

func (q *remoteLogQueue) LastOffset(ctx context.Context) (uint64, error) {
	body := protocol.Buffer{}
	body.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpLastOffset, body.Bytes())
	if err != nil {
		return 0, err
	}
	r := protocol.NewReader(resp)
	return r.ReadU64()
}

func (q *remoteLogQueue) MinOffset(ctx context.Context) (uint64, error) {
	info, err := q.client.Info(ctx)
	if err != nil {
		return 0, err
	}
	qs, ok := info.Queues[q.name]
	if !ok {
		return 0, engine.ErrQueueNotFound
	}
	return qs.MinOffset, nil
}

func (q *remoteLogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) {
	return q.client.Truncate(ctx, q.name, minOffset)
}

func (q *remoteLogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		for curr := startOffset; ; curr++ {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			msg, err := q.Read(ctx, curr)
			if errors.Is(err, engine.ErrOffsetNotFound) {
				return
			}
			if !yield(msg, err) || err != nil {
				return
			}
		}
	}
}

func (q *remoteLogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			last, err := q.LastOffset(ctx)
			if err != nil {
				// transient? retry after sleep
				if isRetryable(err) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Millisecond):
						continue
					}
				}
				yield(engine.Message{}, err)
				return
			}
			if curr >= last {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
					continue
				}
			}
			msg, err := q.Read(ctx, curr)
			if err != nil {
				if isRetryable(err) {
					select {
					case <-ctx.Done():
						return
					case <-time.After(5 * time.Millisecond):
						continue
					}
				}
				yield(engine.Message{}, err)
				return
			}
			if !yield(msg, nil) {
				return
			}
			curr++
		}
	}
}

func (q *remoteLogQueue) Group(name string) logqueue.Group {
	g := &remoteGroup{queue: q}
	g.Base.Name = name
	g.Base.CommitFn = func(ctx context.Context, offset uint64) error {
		body := protocol.Buffer{}
		body.WriteBytes([]byte(q.name))
		body.WriteBytes([]byte(name))
		body.WriteU64(offset)
		_, err := q.client.exchange(ctx, protocol.OpCommit, body.Bytes())
		return err
	}
	return g
}
