package repl

import (
	"bytes"
	"os"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseREPLArgs(t *testing.T) {
	assert.Equal(t, []string{"append", "hello world", "@file.txt"},
		parseREPLArgs(`append "hello world" @file.txt`))
	assert.Equal(t, []string{"append", "", "@file"},
		parseREPLArgs(`append "" @file`))
}

func TestFormatHelpArgs(t *testing.T) {
	assert.NotEmpty(t, formatHelpArgs("<queue> <msg>", 20))
	assert.Len(t, formatHelpArgs("", 10), 10)
}

// TestREPLMirrorsEveryVerb: every REPL-exposed verb has a handler, and
// retired names are unreachable.
func TestREPLMirrorsEveryVerb(t *testing.T) {
	for _, v := range command.Verbs {
		if v.Intercepted || v.Modes&command.ModeREPL == 0 {
			continue
		}
		_, ok := replHandlers[v.Name]
		assert.True(t, ok, "verb %q is missing from the REPL", v.Name)
	}
	for _, name := range []string{"produce", "publish", "pop", "stats", "info"} {
		_, ok := replHandlers[name]
		assert.False(t, ok, "retired verb %q must not be dispatchable", name)
	}
	for _, name := range []string{"use", "exit", "help"} {
		_, ok := replHandlers[name]
		assert.True(t, ok, "session command %q is missing", name)
	}
}

// TestDispatchHelpOnlyViaFlag: `--help` renders a detail page; `help
// enqueue` does not.
func TestDispatchHelpOnlyViaFlag(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()

	sess := &replSession{client: client, ctx: t.Context()}

	out := captureStdout(func() { sess.dispatch("create --help") })
	assert.Contains(t, out, "--max-flush-bytes")

	// `help enqueue` must not render the enqueue detail page.
	out = captureStdout(func() { sess.dispatch("help enqueue") })
	assert.NotContains(t, out, "--scheduled-at")
}

// captureStdout redirects os.Stdout to a pipe for the call and returns what it
// wrote. The reader drains concurrently: a large frame would otherwise fill the
// pipe buffer and block the writer before the function returns.
func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return ""
	}
	os.Stdout = w
	done := make(chan string, 1)
	go func() {
		var out bytes.Buffer
		_, _ = out.ReadFrom(r)
		done <- out.String()
	}()

	fn()
	w.Close()
	os.Stdout = old

	out := <-done
	r.Close()
	return out
}

func TestKindInference(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	ctx := t.Context()
	_, err = client.CreateLogQueue(ctx, "events")
	require.NoError(t, err)
	_, err = client.CreateTaskQueue(ctx, "jobs")
	require.NoError(t, err)

	sess := &replSession{client: client, ctx: ctx}

	assert.Contains(t, captureStdout(func() { sess.handleUse([]string{"events"}) }), "Active queue set to")
	assert.Contains(t, captureStdout(func() { sess.handleUse([]string{"jobs"}) }), "Active queue set to")

	appendLog := captureStdout(func() {
		sess.handleUse([]string{"events"})
		sess.handleAppend([]string{"events", "hello"})
	})
	assert.Contains(t, appendLog, "Appended")

	dequeueTask := captureStdout(func() {
		sess.handleUse([]string{"jobs"})
		sess.handleDequeue([]string{"--timeout", "0"})
	})
	assert.Contains(t, dequeueTask, "No task available.")

	dequeueLog := captureStdout(func() {
		sess.handleUse([]string{"events"})
		sess.handleDequeue([]string{"--timeout", "0"})
	})
	assert.Contains(t, dequeueLog, "dequeue requires a task queue")
}
