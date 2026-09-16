package command_test

import (
	"context"
	"net"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startTokenServer stands up a loopback server in token mode and returns its
// dial target plus a cleanup func.
func startTokenServer(t *testing.T, token string) (target string, cleanup func()) {
	t.Helper()
	n, rel, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	mode, err := server.NewAuthMode(spec.Session{AuthMethod: "token", AuthToken: token})
	require.NoError(t, err)
	srv := server.New(n, server.WithAuth(mode))
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go srv.Serve(ln)
	return "tcp://" + ln.Addr().String(), func() {
		srv.Stop()
		_ = ln.Close()
		_ = rel()
	}
}

// TestOpenClientUsesStoredCredential: a token in the credential store
// authenticates a remote OpenClient with no explicit session flag.
func TestOpenClientUsesStoredCredential(t *testing.T) {
	const token = "prodtoken0123456789"
	target, cleanup := startTokenServer(t, token)
	defer cleanup()

	require.NoError(t, command.SaveToken(target, token))

	sess := spec.BuiltinSession()
	sess.Target = target

	client, err := command.OpenClient(sess)
	require.NoError(t, err)
	defer client.Close()

	queues, err := command.List(context.Background(), client)
	require.NoError(t, err)
	assert.Empty(t, queues)
}

// TestOpenClientRejectsWrongStoredCredential: a stale credential in the store
// surfaces ErrUnauthorized instead of silently passing through.
func TestOpenClientRejectsWrongStoredCredential(t *testing.T) {
	const token = "realtoken0123456789"
	target, cleanup := startTokenServer(t, token)
	defer cleanup()

	require.NoError(t, command.SaveToken(target, "wrongtoken012345678"))

	sess := spec.BuiltinSession()
	sess.Target = target

	client, err := command.OpenClient(sess)
	require.NoError(t, err)
	defer client.Close()

	_, err = command.List(context.Background(), client)
	require.ErrorIs(t, err, mikyu.ErrUnauthorized)
}
