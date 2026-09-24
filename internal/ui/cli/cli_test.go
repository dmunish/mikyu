package cli

import (
	"bytes"
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testApp struct {
	*App
	dir    string
	out    *bytes.Buffer
	errOut *bytes.Buffer
}

func newTestApp(t *testing.T) *testApp {
	t.Helper()
	out, errOut := &bytes.Buffer{}, &bytes.Buffer{}
	return &testApp{
		App:    &App{Stdin: strings.NewReader(""), Stdout: out, Stderr: errOut},
		dir:    t.TempDir(),
		out:    out,
		errOut: errOut,
	}
}

func (a *testApp) run(t *testing.T, args ...string) string {
	t.Helper()
	require.NoError(t, a.runErr(t, args...))
	return a.out.String()
}

func (a *testApp) runErr(t *testing.T, args ...string) error {
	t.Helper()
	a.out.Reset()
	a.errOut.Reset()
	return a.Run("", append([]string{"--dir", a.dir}, args...))
}

func (a *testApp) runRaw(t *testing.T, args ...string) (string, error) {
	t.Helper()
	a.out.Reset()
	a.errOut.Reset()
	err := a.Run("", args)
	return a.out.String(), err
}

func TestAppendRoundTrip(t *testing.T) {
	app := newTestApp(t)

	assert.Contains(t, app.run(t, "create", "--type", "log", "q"), "created queue")
	assert.Contains(t, app.run(t, "append", "q", "hello"), "appended at offset 0")
	assert.Contains(t, app.run(t, "read", "q", "0"), "payload=hello")
}

// TestFlagValueEqualsSign: both --flag value and --flag=value land in
// the same describe output.
func TestFlagValueEqualsSign(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "log", "q",
		"--max-flush-bytes=64",
		"--max-segment", "2048",
		"--max-payload=1024")

	var got mikyu.QueueDescription
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "describe", "q", "--json")), &got))
	require.NotNil(t, got.Spec.Flush.MaxBytes)
	assert.Equal(t, int64(64), *got.Spec.Flush.MaxBytes)
	require.NotNil(t, got.Spec.Storage.MaxSegment)
	assert.Equal(t, int64(2048), *got.Spec.Storage.MaxSegment)
}

func TestPerVerbHelpViaFlagOnly(t *testing.T) {
	app := newTestApp(t)

	out := app.run(t, "create", "--help")
	assert.Contains(t, out, "--max-flush-bytes")

	out = app.run(t, "create", "-h")
	assert.Contains(t, out, "--max-flush-bytes")

	// `create help` must not be treated as a detail-page request.
	_, err := app.runRaw(t, "--dir", app.dir, "create", "help")
	require.ErrorIs(t, err, daemon.ErrUsage)
}

func TestStandaloneHelpPrintsIndex(t *testing.T) {
	app := newTestApp(t)
	out := app.run(t, "help")
	assert.Contains(t, out, "create")
	assert.Contains(t, out, "enqueue")
}

func TestNoLegacyVerbs(t *testing.T) {
	for _, name := range []string{"produce", "publish", "pop", "stats"} {
		_, ok := command.LookupVerb(name)
		assert.False(t, ok, "verb %q must be gone", name)
		_, ok = handlers[name]
		assert.False(t, ok, "verb %q must have no handler", name)
	}
	for _, v := range command.Verbs {
		if v.Intercepted || v.Modes&command.ModeCLI == 0 {
			continue
		}
		_, ok := handlers[v.Name]
		assert.True(t, ok, "verb %q has no CLI handler", v.Name)
	}
}

func TestUpdateImmutableIsUsageError(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "task", "q")

	err := app.runErr(t, "update", "q", "--max-segment", "128")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "max_segment")
}

func TestRetentionShrinkPrompts(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "log", "q", "--max-retention-age", "24h")

	app.Confirm = func(string) (bool, error) { return false, nil }
	assert.Contains(t, app.run(t, "update", "q", "--max-retention-age", "1h"), "aborted")

	app.Confirm = func(string) (bool, error) { t.Error("loosening must not prompt"); return true, nil }
	app.run(t, "update", "q", "--max-retention-age", "48h")

	app.Confirm = func(string) (bool, error) { t.Error("--yes must skip the prompt"); return true, nil }
	app.run(t, "update", "q", "--max-retention-age", "1h", "--yes")

	var got mikyu.QueueDescription
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "describe", "q", "--json")), &got))
	require.NotNil(t, got.Spec.Retention.MaxAge)
	assert.Equal(t, time.Hour, got.Spec.Retention.MaxAge.Duration())
}

func TestTaskRecordRoundTrip(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "task", "q")
	app.run(t, "enqueue", "q", "hello")

	record := strings.TrimSpace(app.run(t, "dequeue", "q", "--json"))
	assert.Contains(t, app.run(t, "ack", "q", record), "acknowledged task")
}

func TestArchiveRoundTrip(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "task", "jobs")
	app.run(t, "enqueue", "jobs", "archive me")

	var taskRecord command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dequeue", "jobs", "--json")), &taskRecord))

	assert.Contains(t, app.run(t, "archive", taskRecord.ID, "--queue", "jobs"), "archived task")
	assert.Contains(t, app.run(t, "read", "jobs-archive", "0"), "archive me")
}

func TestCreateRequiresType(t *testing.T) {
	app := newTestApp(t)
	err := app.runErr(t, "create", "events")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "--type")
}

func TestGlobalFlagsBothForms(t *testing.T) {
	app := newTestApp(t)

	out, err := app.runRaw(t, "--data-dir="+app.dir, "create", "--type", "log", "q")
	require.NoError(t, err)
	assert.Contains(t, out, "created queue")

	out, err = app.runRaw(t, "--dir", app.dir, "--max-connections=4", "list")
	require.NoError(t, err)
	assert.Contains(t, out, "q")
}

func TestConfirmNonInteractive(t *testing.T) {
	app := &App{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}
	_, err := app.confirm("delete queue?", false)
	require.Error(t, err)

	ok, err := app.confirm("delete queue?", true)
	require.NoError(t, err)
	assert.True(t, ok)
}
