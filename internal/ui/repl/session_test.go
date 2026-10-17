package repl

import (
	"net"
	"os"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// deadSession builds a session pointed at a port nothing serves.
func deadSession(t *testing.T) *replSession {
	t.Helper()
	sess := &replSession{session: spec.BuiltinSession(), ctx: t.Context()}
	sess.session.Target = "tcp://127.0.0.1:1"
	sess.target = sess.session.Target
	return sess
}

// TestLazyConnectReportsTheREPLHint: with no daemon and no data dir, the
// first command fails with the REPL's own hint and stays disconnected.
func TestLazyConnectReportsTheREPLHint(t *testing.T) {
	sess := deadSession(t)

	out := captureStdout(func() { sess.handleList(nil) })
	assert.Contains(t, out, "no daemon serving tcp://127.0.0.1:1")
	assert.Contains(t, out, "daemon start")
	assert.Contains(t, out, "open <dir>")
	assert.Nil(t, sess.conn, "a failed connect must not cache a client")

	out = captureStdout(func() { sess.handleList(nil) })
	assert.Contains(t, out, "no daemon serving", "the next command retries the connection")
}

// TestOpenConnectsEmbedded: 'open <dir>' attaches in-process and every later
// command runs without a daemon.
func TestOpenConnectsEmbedded(t *testing.T) {
	sess := deadSession(t)
	dir := t.TempDir()

	out := captureStdout(func() { sess.handleOpen([]string{dir}) })
	assert.Contains(t, out, "in-process")
	require.NotNil(t, sess.conn)
	defer sess.conn.Close()
	assert.Equal(t, dir, sess.dir)

	out = captureStdout(func() { sess.handleList(nil) })
	assert.Contains(t, out, "No queues found.")
}

func TestOpenWithoutDirIsUsage(t *testing.T) {
	sess := deadSession(t)
	out := captureStdout(func() { sess.handleOpen(nil) })
	assert.Contains(t, out, "Usage")
	assert.Nil(t, sess.conn)
}

// TestCloseDetaches: 'close' releases the client; the next command would
// reconnect from scratch.
func TestCloseDetaches(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	sess := &replSession{conn: client, session: spec.BuiltinSession(), ctx: t.Context()}

	out := captureStdout(func() { sess.handleClose(nil) })
	assert.Contains(t, out, "Closed")
	assert.Nil(t, sess.conn)

	out = captureStdout(func() { sess.handleClose(nil) })
	assert.Contains(t, out, "Not connected")
}

// TestDispatchRoutesSessionCommands: open, close, and daemon are session
// commands, dispatched before the verb table.
func TestDispatchRoutesSessionCommands(t *testing.T) {
	sess := deadSession(t)

	out := captureStdout(func() { sess.dispatch("open") })
	assert.Contains(t, out, "Usage")

	out = captureStdout(func() { sess.dispatch("daemon run") })
	assert.Contains(t, out, "daemon start")

	out = captureStdout(func() { sess.dispatch("daemon bogus") })
	assert.Contains(t, out, "Usage")

	out = captureStdout(func() { sess.dispatch("daemon status") })
	assert.Contains(t, out, "no daemon serving tcp://127.0.0.1:1")
}

// TestDaemonStartGuards: a live target is reported instead of spawning a
// second daemon, and --dir defaults to the directory the REPL runs in.
func TestDaemonStartGuards(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	target := "tcp://" + ln.Addr().String()

	sess := deadSession(t)
	sess.session.Target = target
	sess.target = target

	out := captureStdout(func() { sess.handleDaemonStart(nil) })
	assert.Contains(t, out, "already serving")

	wd, err := os.Getwd()
	require.NoError(t, err)
	assert.Equal(t, wd, defaultDaemonDir())
}

// TestHelpListsSessionCommands: the help index is followed by the session
// commands the verb table does not carry.
func TestHelpListsSessionCommands(t *testing.T) {
	sess := deadSession(t)
	out := captureStdout(func() { sess.handleHelp(nil) })
	assert.Contains(t, out, "Work in-process on a data directory")
	assert.Contains(t, out, "Launch a background daemon")
}
