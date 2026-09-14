package server

import (
	"context"
	"time"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// dispatch routes opcodes to handlers.
func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (respOp byte, respBody []byte, shouldShutdown bool) {
	start := time.Now()
	defer func() { s.recordOp(op, start) }()

	switch op {
	case protocol.OpCreateQueue:
		respOp, respBody = s.handleCreateQueue(ctx, body)
	case protocol.OpDeleteQueue:
		respOp, respBody = s.handleDeleteQueue(body)
	case protocol.OpListQueues:
		respOp, respBody = s.handleListQueues()
	case protocol.OpUpdateQueue:
		respOp, respBody = s.handleUpdateQueue(ctx, body)
	case protocol.OpDescribeQueue:
		respOp, respBody = s.handleDescribeQueue(ctx, body)
	case protocol.OpTruncate:
		respOp, respBody = s.handleTruncate(ctx, body)
	case protocol.OpMetrics:
		respOp, respBody = s.handleMetrics(ctx)

	case protocol.OpAppend:
		respOp, respBody = s.handleAppend(ctx, body)
	case protocol.OpAppendBatch:
		respOp, respBody = s.handleAppendBatch(ctx, body)
	case protocol.OpRead:
		respOp, respBody = s.handleRead(ctx, body)
	case protocol.OpReadBatch:
		respOp, respBody = s.handleReadBatch(ctx, body)
	case protocol.OpLastOffset:
		respOp, respBody = s.handleLastOffset(ctx, body)
	case protocol.OpCommit:
		respOp, respBody = s.handleCommit(ctx, body)
	case protocol.OpCommitted:
		respOp, respBody = s.handleCommitted(ctx, body)
	case protocol.OpFetch:
		respOp, respBody = s.handleFetch(ctx, body)

	case protocol.OpEnqueue:
		respOp, respBody = s.handleEnqueue(ctx, body)
	case protocol.OpDequeue:
		respOp, respBody = s.handleDequeue(ctx, body)
	case protocol.OpAck:
		respOp, respBody = s.handleAck(ctx, body)
	case protocol.OpNack:
		respOp, respBody = s.handleNack(ctx, body)
	case protocol.OpExtend:
		respOp, respBody = s.handleExtend(ctx, body)
	case protocol.OpDeadLetters:
		respOp, respBody = s.handleDeadLetters(ctx, body)
	case protocol.OpDeadLetterAck:
		respOp, respBody = s.handleDeadLetterAck(ctx, body)

	case protocol.OpShutdown:
		return protocol.OpShutdown, nil, true
	default:
		respOp, respBody = opErr(protocol.ErrCodeUnknown, "unknown opcode")
	}
	return respOp, respBody, false
}
