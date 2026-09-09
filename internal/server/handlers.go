package server

import (
	"context"
	"errors"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// dispatch routes opcodes to handlers.
func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (respOp byte, respBody []byte, shouldShutdown bool) {
	switch op {
	case protocol.OpCreateQueue:
		respOp, respBody = s.handleCreateQueue(body)
	case protocol.OpProduce:
		respOp, respBody = s.handleProduce(ctx, body)
	case protocol.OpProduceBatch:
		respOp, respBody = s.handleProduceBatch(ctx, body)
	case protocol.OpRead:
		respOp, respBody = s.handleRead(ctx, body)
	case protocol.OpReadBatch:
		respOp, respBody = s.handleReadBatch(ctx, body)
	case protocol.OpTail:
		respOp, respBody = s.handleTail(ctx, body)
	case protocol.OpCommitOffset:
		respOp, respBody = s.handleCommitOffset(ctx, body)
	case protocol.OpFetchOffset:
		respOp, respBody = s.handleFetchOffset(ctx, body)
	case protocol.OpListQueues:
		respOp, respBody = s.handleListQueues()
	case protocol.OpDeleteQueue:
		respOp, respBody = s.handleDeleteQueue(body)
	case protocol.OpStats:
		respOp, respBody = s.handleStats()
	case protocol.OpTaskEnqueue:
		respOp, respBody = s.handleTaskEnqueue(ctx, body)
	case protocol.OpTaskDequeue:
		respOp, respBody = s.handleTaskDequeue(ctx, body)
	case protocol.OpTaskAck:
		respOp, respBody = s.handleTaskAck(ctx, body)
	case protocol.OpTaskFail:
		respOp, respBody = s.handleTaskFail(ctx, body)
	case protocol.OpTaskExtend:
		respOp, respBody = s.handleTaskExtend(ctx, body)
	case protocol.OpTaskDeadLetters:
		respOp, respBody = s.handleTaskDeadLetters(ctx, body)
	case protocol.OpTaskDeadLetterAck:
		respOp, respBody = s.handleTaskDeadLetterAck(ctx, body)
	case protocol.OpTruncate:
		respOp, respBody = s.handleTruncate(ctx, body)
	case protocol.OpShutdown:
		return protocol.OpShutdown, nil, true
	case protocol.OpTaskAckByID:
		respOp, respBody = s.handleTaskAckByID(ctx, body)
	case protocol.OpTaskFailByID:
		respOp, respBody = s.handleTaskFailByID(ctx, body)
	case protocol.OpTaskExtendByID:
		respOp, respBody = s.handleTaskExtendByID(ctx, body)
	case protocol.OpTaskDeadLetterAckByID:
		respOp, respBody = s.handleTaskDeadLetterAckByID(ctx, body)
	default:
		respOp, respBody = opErr(protocol.ErrCodeUnknown, "unknown opcode")
	}
	return respOp, respBody, false
}

func (s *Server) handleCreateQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	cfg := engine.Config{}
	if batch, err := r.ReadU32(); err == nil {
		cfg.BatchSize = int(batch)
		maxInterval, e1 := r.ReadU64()
		minInterval, e2 := r.ReadU64()
		maxSeg, e3 := r.ReadU64()
		maxMsg, e4 := r.ReadU64()
		maxAge, e5 := r.ReadU64()
		encrypted, e6 := r.ReadU32()
		fsync, e7 := r.ReadU32()
		if e1 != nil || e2 != nil || e3 != nil || e4 != nil || e5 != nil || e6 != nil || e7 != nil {
			return opErr(protocol.ErrCodeUnknown, "invalid queue configuration")
		}
		cfg.MaxInterval = time.Duration(maxInterval)
		cfg.MinInterval = time.Duration(minInterval)
		cfg.MaxSegmentBytes = int64(maxSeg)
		cfg.MaxMessageBytes = int64(maxMsg)
		cfg.MaxSegmentAge = time.Duration(maxAge)
		cfg.Encrypted = encrypted != 0
		cfg.Fsync = fsync != 0
	}
	_, err = s.mgr.CreateQueue(string(name), cfg)
	if err != nil {
		if errors.Is(err, engine.ErrQueueExists) {
			return opErr(protocol.ErrCodeQueueExists, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpCreateQueue, nil
}

func (s *Server) handleProduce(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	payload, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	off, err := q.Produce(ctx, payload)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpProduce, buf.Bytes()
}

func (s *Server) handleProduceBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	count, err := r.ReadU32()
	if err != nil || count > 65536 {
		return opErr(protocol.ErrCodeUnknown, "invalid batch count")
	}
	payloads := make([][]byte, count)
	for i := range count {
		payloads[i], _ = r.ReadBytes()
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	offs, err := q.ProduceBatch(ctx, payloads)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(offs)))
	for _, off := range offs {
		buf.WriteU64(off)
	}
	return protocol.OpProduceBatch, buf.Bytes()
}

func (s *Server) handleRead(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	msg, err := q.Read(ctx, off)
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		if errors.Is(err, engine.ErrOffsetTruncated) {
			return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(msg.Offset)
	buf.WriteBytes([]byte(msg.ID))
	buf.WriteU64(uint64(msg.Timestamp.UnixNano()))
	buf.WriteBytes(msg.Payload)
	return protocol.OpRead, buf.Bytes()
}

func (s *Server) handleReadBatch(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	start, _ := r.ReadU64()
	count, err := r.ReadU32()
	if err != nil || count > 65536 {
		return opErr(protocol.ErrCodeUnknown, "invalid batch count")
	}
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	msgs, err := q.ReadBatch(ctx, start, int(count))
	if err != nil {
		if errors.Is(err, engine.ErrOffsetNotFound) {
			return opErr(protocol.ErrCodeOffsetNotFound, err.Error())
		}
		if errors.Is(err, engine.ErrOffsetTruncated) {
			return opErr(protocol.ErrCodeOffsetTruncated, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(msgs)))
	for _, msg := range msgs {
		buf.WriteU64(msg.Offset)
		buf.WriteBytes([]byte(msg.ID))
		buf.WriteU64(uint64(msg.Timestamp.UnixNano()))
		buf.WriteBytes(msg.Payload)
	}
	return protocol.OpReadBatch, buf.Bytes()
}

func (s *Server) handleTail(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	last, err := q.LastOffset(ctx)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(last)
	return protocol.OpTail, buf.Bytes()
}

func (s *Server) handleCommitOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	off, _ := r.ReadU64()
	q, err := s.getOrCreateLogQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	if err := q.CommitOffset(ctx, string(gname), off); err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	return protocol.OpCommitOffset, nil
}

func (s *Server) handleFetchOffset(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	qname, _ := r.ReadBytes()
	gname, _ := r.ReadBytes()
	q, err := s.getOrCreateLogQueue(string(qname))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	off, err := q.FetchOffset(ctx, string(gname))
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	buf := protocol.Buffer{}
	buf.WriteU64(off)
	return protocol.OpFetchOffset, buf.Bytes()
}

func (s *Server) handleListQueues() (byte, []byte) {
	names := s.mgr.ListQueues()
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(len(names)))
	for _, n := range names {
		buf.WriteBytes([]byte(n))
	}
	return protocol.OpListQueues, buf.Bytes()
}

func (s *Server) handleDeleteQueue(body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, _ := r.ReadBytes()
	s.mu.Lock()
	delete(s.queues, string(name))
	if tq := s.tasks[string(name)]; tq != nil {
		_ = tq.Close()
	}
	delete(s.tasks, string(name))
	s.retention.UnregisterTarget(string(name))
	if err := s.mgr.DeleteQueue(string(name)); err != nil {
		s.mu.Unlock()

		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	s.mu.Unlock()
	return protocol.OpDeleteQueue, nil
}

func (s *Server) handleTruncate(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	minOffset, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	q, err := s.getOrCreateLogQueue(string(name))
	if err != nil {
		if errors.Is(err, engine.ErrQueueNotFound) {
			return opErr(protocol.ErrCodeQueueNotFound, err.Error())
		}
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	_ = q // The log driver is also registered with the retention coordinator.

	// Materialize the task driver for manual truncation so durable task state
	// is included even after a daemon restart before a task client connects.
	if _, err := s.getOrCreateTaskQueue(string(name)); err != nil && !errors.Is(err, engine.ErrQueueNotFound) {
		return taskErr(err)
	}

	reclaimed, err := s.retention.ReconcileQueueAt(ctx, string(name), minOffset)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}

	buf := protocol.Buffer{}
	buf.WriteU64(uint64(reclaimed))
	return protocol.OpTruncate, buf.Bytes()
}

func (s *Server) handleStats() (byte, []byte) {
	st := s.mgr.Stats()
	buf := protocol.Buffer{}
	buf.WriteU32(uint32(st.ActiveQueues))
	buf.WriteU64(uint64(st.TotalBytes))
	buf.WriteU32(uint32(len(st.Queues)))
	for name, qs := range st.Queues {
		buf.WriteBytes([]byte(name))
		buf.WriteU64(qs.MessageCount)
		buf.WriteU32(uint32(qs.SegmentCount))
		buf.WriteU64(uint64(qs.SizeBytes))
		buf.WriteU64(qs.LastOffset)
		buf.WriteU64(qs.MinOffset)
	}
	return protocol.OpStats, buf.Bytes()
}

func opErr(code byte, msg string) (byte, []byte) {
	return protocol.OpError, protocol.EncodeError(code, msg)
}

func taskErr(err error) (byte, []byte) {
	switch {
	case errors.Is(err, engine.ErrQueueNotFound):
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotAvailable):
		return opErr(protocol.ErrCodeTaskNotAvailable, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotFound):
		return opErr(protocol.ErrCodeTaskNotFound, err.Error())
	case errors.Is(err, taskqueue.ErrTaskNotInflight):
		return opErr(protocol.ErrCodeTaskNotInflight, err.Error())
	case errors.Is(err, taskqueue.ErrTaskStale):
		return opErr(protocol.ErrCodeTaskStale, err.Error())
	case errors.Is(err, taskqueue.ErrTaskClosed):
		return opErr(protocol.ErrCodeTaskClosed, err.Error())
	case errors.Is(err, taskqueue.ErrInvalidTask), errors.Is(err, taskqueue.ErrCheckpointCorrupt):
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	case errors.Is(err, taskqueue.ErrDeadLetterNotFound):
		return opErr(protocol.ErrCodeDeadLetterNotFound, err.Error())
	case errors.Is(err, taskqueue.ErrDeadLetterStale):
		return opErr(protocol.ErrCodeDeadLetterStale, err.Error())
	default:
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
}

func (s *Server) handleTaskEnqueue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	maxAttempts, err := r.ReadU32()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	visibilityRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	scheduledRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	opts := []taskqueue.TaskOption{
		taskqueue.WithMaxAttempts(maxAttempts),
		taskqueue.WithVisibilityTimeout(time.Duration(int64(visibilityRaw))),
		taskqueue.WithScheduleAt(time.Unix(0, int64(scheduledRaw))),
	}
	if scheduledRaw == 0 {
		opts[2] = taskqueue.WithScheduleAt(time.Time{})
	}
	t, err := q.Enqueue(ctx, payload, opts...)
	if err != nil {
		return taskErr(err)
	}
	var b protocol.Buffer
	protocol.EncodeTask(&b, t)
	return protocol.OpTaskEnqueue, b.Bytes()
}

func (s *Server) handleTaskDequeue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	t, err := q.DequeueNow(ctx)
	if err != nil {
		return taskErr(err)
	}
	var b protocol.Buffer
	protocol.EncodeTask(&b, t)
	return protocol.OpTaskDequeue, b.Bytes()
}

func (s *Server) handleTaskAck(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	t, err := protocol.DecodeTask(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	if err = q.Ack(ctx, t); err != nil {
		return taskErr(err)
	}
	return protocol.OpTaskAck, nil
}

func (s *Server) handleTaskAckByID(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)

	queue, taskID, err := protocol.DecodeTaskID(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(queue)
	if err != nil {
		return taskErr(err)
	}

	if err := q.AckID(ctx, taskID); err != nil {
		return taskErr(err)
	}

	return protocol.OpTaskAckByID, nil
}

func (s *Server) handleTaskFail(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	t, err := protocol.DecodeTask(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	retryRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	if err = q.Fail(ctx, t, taskqueue.WithRetryAfter(time.Duration(int64(retryRaw)))); err != nil {
		return taskErr(err)
	}
	return protocol.OpTaskFail, nil
}

func (s *Server) handleTaskFailByID(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)

	queue, taskID, err := protocol.DecodeTaskID(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	hasRetryRaw, err := r.ReadU32()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	retryRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(queue)
	if err != nil {
		return taskErr(err)
	}

	var opts []taskqueue.FailOption
	if hasRetryRaw != 0 {
		opts = append(opts, taskqueue.WithRetryAfter(time.Duration(int64(retryRaw))))
	}

	if err := q.FailID(ctx, taskID, opts...); err != nil {
		return taskErr(err)
	}

	return protocol.OpTaskFailByID, nil
}

func (s *Server) handleTaskExtend(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	t, err := protocol.DecodeTask(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	timeoutRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	if err := q.Extend(ctx, t, time.Duration(int64(timeoutRaw))); err != nil {
		return taskErr(err)
	}
	return protocol.OpTaskExtend, nil
}

func (s *Server) handleTaskExtendByID(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)

	queue, taskID, err := protocol.DecodeTaskID(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	timeoutRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(queue)
	if err != nil {
		return taskErr(err)
	}

	if err := q.ExtendID(ctx, taskID, time.Duration(int64(timeoutRaw))); err != nil {
		return taskErr(err)
	}

	return protocol.OpTaskExtendByID, nil
}

func (s *Server) handleTaskDeadLetters(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	tasks, err := q.DeadLetters(ctx)
	if err != nil {
		return taskErr(err)
	}
	var b protocol.Buffer
	protocol.EncodeTaskList(&b, tasks)
	return protocol.OpTaskDeadLetters, b.Bytes()
}

func (s *Server) handleTaskDeadLetterAck(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	task, err := protocol.DecodeTask(r)
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return opErr(protocol.ErrCodeQueueNotFound, err.Error())
	}
	if err := q.AckDeadLetter(ctx, task); err != nil {
		return taskErr(err)
	}
	return protocol.OpTaskDeadLetterAck, nil
}

func (s *Server) handleTaskDeadLetterAckByID(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)

	queue, taskID, err := protocol.DecodeTaskID(r)
	if err != nil {
		return opErr(protocol.ErrCodeInvalidTask, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(queue)
	if err != nil {
		return taskErr(err)
	}

	if err := q.AckDeadLetterID(ctx, taskID); err != nil {
		return taskErr(err)
	}

	return protocol.OpTaskDeadLetterAckByID, nil
}
