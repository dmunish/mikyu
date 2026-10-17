package server

import (
	"context"
	"net"
	"time"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// dispatch routes opcodes to handlers. streamed is true when the handler
// already wrote its own response frame (the zero-copy read path).
func (s *Server) dispatch(ctx context.Context, conn net.Conn, op byte, body []byte) (respOp byte, respBody []byte, shouldShutdown bool, streamed bool) {
	start := time.Now()
	defer func() { s.recordOp(op, start) }()

	switch op {
	case protocol.OpCreate:
		respOp, respBody = s.handleCreate(ctx, body)
	case protocol.OpDelete:
		respOp, respBody = s.handleDelete(body)
	case protocol.OpList:
		respOp, respBody = s.handleList(ctx)
	case protocol.OpUpdate:
		respOp, respBody = s.handleUpdate(ctx, body)
	case protocol.OpDescribe:
		respOp, respBody = s.handleDescribe(ctx, body)
	case protocol.OpTruncate:
		respOp, respBody = s.handleTruncate(ctx, body)
	case protocol.OpMetrics:
		respOp, respBody = s.handleMetrics(ctx)
	case protocol.OpSchema:
		respOp, respBody = s.handleSchema()
	case protocol.OpLogs:
		respOp, respBody, streamed = s.handleLogs(ctx, conn, body)

	case protocol.OpAppend:
		respOp, respBody = s.handleAppend(ctx, body)
	case protocol.OpAppendBatch:
		respOp, respBody = s.handleAppendBatch(ctx, body)
	case protocol.OpRead:
		respOp, respBody = s.handleRead(ctx, body)
	case protocol.OpReadBatch:
		respOp, respBody, streamed = s.handleReadBatch(ctx, conn, body)
	case protocol.OpLastOffset:
		respOp, respBody = s.handleLastOffset(ctx, body)
	case protocol.OpMinOffset:
		respOp, respBody = s.handleMinOffset(ctx, body)
	case protocol.OpCommit:
		respOp, respBody = s.handleCommit(ctx, body)
	case protocol.OpCommitted:
		respOp, respBody = s.handleCommitted(ctx, body)
	case protocol.OpFetch:
		respOp, respBody, streamed = s.handleFetch(ctx, conn, body)

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
	case protocol.OpArchive:
		respOp, respBody = s.handleArchive(ctx, body)

	case protocol.OpShutdown:
		return protocol.OpShutdown, nil, true, false
	default:
		respOp, respBody = opErr(protocol.ErrCodeUnknown, "unknown opcode")
	}
	return respOp, respBody, false, streamed
}
