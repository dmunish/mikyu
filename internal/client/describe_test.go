package client

import (
	"encoding/json"
	"net"
	"sort"
	"testing"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newLoopbackServer starts a server on a node with empty session defaults and
// returns a remote client for it.
func newLoopbackServer(t *testing.T) *remoteClient {
	t.Helper()
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestDescribeAfterCreateWithFlags: the embedded and remote clients store the
// same sparse spec, so DescribeQueue produces identical JSON.
func TestDescribeAfterCreateWithFlags(t *testing.T) {
	ctx := t.Context()
	opts := []QueueOption{WithSegmentSize(128), WithMaxMessageBytes(128)}

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	require.NoError(t, emb.CreateQueue(ctx, "q", opts...))
	embDesc, err := emb.DescribeQueue(ctx, "q")
	require.NoError(t, err)
	embJSON, err := json.Marshal(embDesc)
	require.NoError(t, err)

	rem := newLoopbackServer(t)
	require.NoError(t, rem.CreateQueue(ctx, "q", opts...))
	remDesc, err := rem.DescribeQueue(ctx, "q")
	require.NoError(t, err)
	remJSON, err := json.Marshal(remDesc)
	require.NoError(t, err)

	require.JSONEq(t, string(embJSON), string(remJSON))
}

// TestInfoShape: Info carries the same shape through the embedded and remote
// clients, and survives a JSON round-trip.
func TestInfoShape(t *testing.T) {
	ctx := t.Context()

	emb, err := NewEmbedded(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = emb.Close() })
	require.NoError(t, emb.CreateQueue(ctx, "q"))
	embInfo, err := emb.Info(ctx)
	require.NoError(t, err)

	assert.NotEmpty(t, embInfo.Version)
	assert.Equal(t, 1, embInfo.ActiveQueues)
	assert.Contains(t, embInfo.Queues, "q")
	assert.Greater(t, embInfo.UptimeNs, int64(0))
	assert.NotEmpty(t, embInfo.DataDir)

	data, err := json.Marshal(embInfo)
	require.NoError(t, err)
	var back Info
	require.NoError(t, json.Unmarshal(data, &back))
	assert.Equal(t, embInfo, back)

	rem := newLoopbackServer(t)
	require.NoError(t, rem.CreateQueue(ctx, "q"))
	remInfo, err := rem.Info(ctx)
	require.NoError(t, err)

	assert.Equal(t, jsonKeys(t, embInfo), jsonKeys(t, remInfo))
	assert.Equal(t, jsonKeys(t, embInfo.Queues["q"]), jsonKeys(t, remInfo.Queues["q"]))
}

func jsonKeys(t *testing.T, v any) []string {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)
	var m map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(data, &m))
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
