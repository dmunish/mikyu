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
	args := parseREPLArgs(`append "hello world" @file.txt`)
	assert.Equal(t, []string{"append", "hello world", "@file.txt"}, args)
}

func TestParseREPLArgsEmptyQuotes(t *testing.T) {
	args := parseREPLArgs(`append "" @file`)
	assert.Equal(t, []string{"append", "", "@file"}, args)
}

func TestFormatHelpArgs(t *testing.T) {
	formatted := formatHelpArgs("<queue> <msg>", 20)
	assert.NotEmpty(t, formatted)

	empty := formatHelpArgs("", 10)
	assert.Equal(t, 10, len(empty))
}

// The REPL mirrors every verb the CLI has, plus its session commands.
func TestREPLMatchesCLIVerbs(t *testing.T) {
	for _, v := range command.Verbs {
		_, ok := replHandlers[v.Name]
		assert.True(t, ok, "verb %q is missing from the REPL", v.Name)
	}

	for _, name := range []string{"produce", "publish", "pop", "stats"} {
		_, ok := replHandlers[name]
		assert.False(t, ok, "retired verb %q must not be dispatchable", name)
	}

	for _, name := range []string{"use"} {
		_, ok := replHandlers[name]
		assert.True(t, ok, "session command %q is missing", name)
	}
}

func TestREPLMetricsVerb(t *testing.T) {
	_, ok := replHandlers["metrics"]
	assert.True(t, ok, "metrics must be dispatchable in REPL")
}

func TestREPLNoInfoVerb(t *testing.T) {
	_, ok := replHandlers["info"]
	assert.False(t, ok, "info must not be dispatchable in REPL")
}

func captureStdout(fn func()) string {
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		return ""
	}
	os.Stdout = w
	fn()
	w.Close()
	os.Stdout = old
	var out bytes.Buffer
	_, _ = out.ReadFrom(r)
	r.Close()
	return out.String()
}

func TestREPLKindInference(t *testing.T) {
	client, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer client.Close()
	ctx := t.Context()
	_, err = client.CreateLogQueue(ctx, "events")
	require.NoError(t, err)
	_, err = client.CreateTaskQueue(ctx, "jobs")
	require.NoError(t, err)

	sess := &replSession{client: client, ctx: ctx}

	useLog := captureStdout(func() { sess.handleUse([]string{"events"}) })
	assert.Contains(t, useLog, "Active queue set to")

	useTask := captureStdout(func() { sess.handleUse([]string{"jobs"}) })
	assert.Contains(t, useTask, "Active queue set to")

	appendLog := captureStdout(func() { sess.handleUse([]string{"events"}); sess.handleAppend([]string{"events", "hello"}) })
	assert.Contains(t, appendLog, "Appended")

	dequeueTask := captureStdout(func() { sess.handleUse([]string{"jobs"}); sess.handleDequeue([]string{"--timeout", "0"}) })
	assert.Contains(t, dequeueTask, "No task available.")

	dequeueLog := captureStdout(func() { sess.handleUse([]string{"events"}); sess.handleDequeue([]string{"--timeout", "0"}) })
	assert.Contains(t, dequeueLog, "dequeue requires a task queue")

	tailTask := captureStdout(func() { sess.handleUse([]string{"jobs"}); sess.handleTail([]string{"jobs"}) })
	assert.Contains(t, tailTask, "tail requires a log queue")
}
