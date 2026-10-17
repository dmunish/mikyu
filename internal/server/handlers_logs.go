package server

import (
	"context"
	"net"

	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// handleLogs streams the node's log ring to the connection. The request body
// is u8 follow: without it the handler replays the ring and closes with an
// empty end frame; with it the stream continues until the client detaches,
// the follower falls behind, or the daemon stops. The handler writes its own
// frames, so dispatch treats it as streamed.
func (s *Server) handleLogs(ctx context.Context, conn net.Conn, body []byte) (byte, []byte, bool) {
	r := protocol.NewReader(body)
	follow, err := protocol.DecodeLogsSubscribe(r)
	if err != nil {
		respOp, respBody := opErr(protocol.ErrCodeUnknown, "malformed logs request")
		return respOp, respBody, false
	}

	backlog, ch := s.node.LogTap().Subscribe()
	defer s.node.LogTap().Unsubscribe(ch)

	for _, line := range backlog {
		if err := protocol.WriteFrame(conn, protocol.OpLogs, line); err != nil {
			return 0, nil, true // the connection is gone; nothing left to report
		}
	}
	if !follow {
		_ = protocol.WriteFrame(conn, protocol.OpLogs, nil) // empty frame: end of stream
		return 0, nil, true
	}

	for {
		select {
		case line, ok := <-ch:
			if !ok {
				// The tap dropped this follower for falling behind; end the
				// stream so the client can reattach and replay.
				_ = protocol.WriteFrame(conn, protocol.OpLogs, nil)
				return 0, nil, true
			}
			if err := protocol.WriteFrame(conn, protocol.OpLogs, line); err != nil {
				return 0, nil, true
			}
		case <-ctx.Done():
			// The client detached or the connection died.
			return 0, nil, true
		}
	}
}
