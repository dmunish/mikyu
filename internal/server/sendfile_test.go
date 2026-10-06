package server

import (
	"bufio"
	"bytes"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

func TestStreamRecordSpansMatchesWriteRecordList(t *testing.T) {
	dir := t.TempDir()
	payloadPath := filepath.Join(dir, "payload.bin")
	big := make([]byte, 64<<10)
	small := []byte("tiny payload")
	for i := range big {
		big[i] = byte(i)
	}
	f, err := os.Create(payloadPath)
	require.NoError(t, err)
	_, err = f.Write(big)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	sf, err := os.Open(payloadPath)
	require.NoError(t, err)
	defer sf.Close()
	var mu sync.Mutex

	now := time.Unix(1700000000, 123).UTC()
	spans := []engine.PayloadSpan{
		{Offset: 7, ID: "id-7", Timestamp: now, Group: []byte("g"), PayloadLen: len(big), File: sf, FileOff: 0, SendMu: &mu},
		{Offset: 8, ID: "id-8", Timestamp: now, Group: nil, PayloadLen: len(small), Payload: small},
	}
	want := []protocol.Record{
		{Offset: 7, ID: "id-7", Timestamp: now, Group: []byte("g"), Payload: big},
		{Offset: 8, ID: "id-8", Timestamp: now, Group: nil, Payload: small},
	}
	var wantBuf protocol.Buffer
	protocol.WriteRecordList(&wantBuf, want)

	pr, pw := net.Pipe()
	defer pr.Close()
	defer pw.Close()
	errCh := make(chan error, 1)
	go func() {
		errCh <- streamRecordSpans(pw, protocol.OpFetch, spans)
	}()

	op, body, err := protocol.ReadFrame(bufio.NewReader(pr))
	require.NoError(t, err)
	require.NoError(t, <-errCh)
	assert.Equal(t, protocol.OpFetch, op)
	assert.Equal(t, wantBuf.Bytes(), body)

	got, err := protocol.ReadRecordList(protocol.NewReader(body))
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for i := range got {
		assert.Equal(t, want[i].Offset, got[i].Offset)
		assert.Equal(t, want[i].ID, got[i].ID)
		assert.True(t, want[i].Timestamp.Equal(got[i].Timestamp))
		assert.True(t, bytes.Equal(want[i].Group, got[i].Group))
		assert.True(t, bytes.Equal(want[i].Payload, got[i].Payload))
	}
}

func TestFetchOverTCPPrefersSendfile(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	srv := New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv.listener = ln
	go srv.Serve(ln)
	defer srv.Stop()

	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn.Close()
	rd := bufio.NewReader(conn)

	require.NoError(t, protocol.WriteFrame(conn, protocol.OpCreate, createFrame(t, node.KindLog, "sendfile-q", spec.Spec{})))
	op, _, err := protocol.ReadFrame(rd)
	require.NoError(t, err)
	require.Equal(t, protocol.OpCreate, op)

	const count = 8
	payloads := make([][]byte, count)
	for i := range payloads {
		payloads[i] = make([]byte, 64<<10)
		for j := range payloads[i] {
			payloads[i][j] = byte(i + j)
		}
	}
	var ab protocol.Buffer
	ab.WriteBytes([]byte("sendfile-q"))
	ab.WriteU32(count)
	ab.WriteBytes(nil)
	for _, p := range payloads {
		ab.WriteBytes(p)
	}
	require.NoError(t, protocol.WriteFrame(conn, protocol.OpAppendBatch, ab.Bytes()))
	op, _, err = protocol.ReadFrame(rd)
	require.NoError(t, err)
	require.Equal(t, protocol.OpAppendBatch, op)

	var hookMu sync.Mutex
	var sent []int
	sendfileHook = func(n int) {
		hookMu.Lock()
		defer hookMu.Unlock()
		sent = append(sent, n)
	}
	defer func() { sendfileHook = nil }()

	var fb protocol.Buffer
	protocol.EncodeFetch(&fb, "sendfile-q", 0, count, 0)
	require.NoError(t, protocol.WriteFrame(conn, protocol.OpFetch, fb.Bytes()))
	op, body, err := protocol.ReadFrame(rd)
	require.NoError(t, err)
	require.Equal(t, protocol.OpFetch, op)

	recs, err := protocol.ReadRecordList(protocol.NewReader(body))
	require.NoError(t, err)
	require.Len(t, recs, count)
	for i := range recs {
		assert.Equal(t, uint64(i), recs[i].Offset)
		assert.Equal(t, payloads[i], recs[i].Payload)
	}
	hookMu.Lock()
	defer hookMu.Unlock()
	if !sendfileSupported() {
		assert.Empty(t, sent)
		return
	}
	assert.Len(t, sent, count)
	for _, sn := range sent {
		assert.Equal(t, 64<<10, sn)
	}
}
