package server

import (
	"errors"
	"io"
	"net"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// sendfileHook, when non-nil, receives the byte count of every zero-copy payload send.
var sendfileHook func(n int)

// streamSpans writes q's immediately available records to conn as one frame,
// sending file-backed payloads with the kernel's zero-copy path. ok is false
// when the caller should serve the request through the copy path.
func (s *Server) streamSpans(conn net.Conn, q logqueue.Log, op byte, start uint64, count int) bool {
	if count <= 0 {
		return false
	}
	if _, ok := conn.(*net.TCPConn); !ok {
		return false
	}
	if !sendfileSupported() {
		return false
	}
	ss, ok := q.(logqueue.SpanSource)
	if !ok {
		return false
	}
	spans, err := ss.PayloadSpans(start, count)
	if err != nil || len(spans) == 0 {
		return false
	}
	if err := streamRecordSpans(conn, op, spans); err != nil {
		_ = conn.Close()
	}
	return true
}

// streamRecordSpans writes one record-list frame to conn, streaming each file-backed payload straight from disk.
func streamRecordSpans(conn net.Conn, op byte, spans []engine.PayloadSpan) error {
	total := 5
	for i := range spans {
		total += protocol.RecordFramingLen(spans[i].ID, spans[i].Group) + spans[i].PayloadLen
	}
	if uint32(total) > protocol.MaxFrameBytes {
		return errors.New("streamed frame exceeds maximum size")
	}
	if err := protocol.WriteFrameHeader(conn, uint32(total)); err != nil {
		return err
	}
	if _, err := conn.Write([]byte{op}); err != nil {
		return err
	}

	var buf protocol.Buffer
	buf.WriteU32(uint32(len(spans)))
	for i := range spans {
		sp := &spans[i]
		if sp.File == nil {
			buf.WriteRecordFraming(sp.Offset, sp.ID, sp.Timestamp, sp.Group, len(sp.Payload))
			buf.WriteRaw(sp.Payload)
			continue
		}
		buf.WriteRecordFraming(sp.Offset, sp.ID, sp.Timestamp, sp.Group, sp.PayloadLen)
		if err := flushBuf(conn, &buf); err != nil {
			return err
		}
		if err := sendPayload(conn, sp); err != nil {
			return err
		}
	}
	return flushBuf(conn, &buf)
}

// flushBuf writes any buffered framing bytes to conn and empties the buffer.
func flushBuf(conn net.Conn, buf *protocol.Buffer) error {
	if len(buf.Bytes()) == 0 {
		return nil
	}
	_, err := conn.Write(buf.Bytes())
	buf.Reset()
	return err
}

// sendPayload transmits one file-backed payload without routing it through userland.
func sendPayload(conn net.Conn, sp *engine.PayloadSpan) error {
	sp.SendMu.Lock()
	defer sp.SendMu.Unlock()
	if _, err := sp.File.Seek(sp.FileOff, io.SeekStart); err != nil {
		return err
	}
	if sendfileHook != nil {
		sendfileHook(sp.PayloadLen)
	}
	written, err := io.CopyN(conn, sp.File, int64(sp.PayloadLen))
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if written != int64(sp.PayloadLen) {
		return io.ErrUnexpectedEOF
	}
	return nil
}
