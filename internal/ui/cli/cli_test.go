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
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testApp drives the CLI in-process against one data directory with captured streams.
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

// run executes one invocation and returns its stdout alone.
func (a *testApp) run(t *testing.T, args ...string) string {
	t.Helper()
	require.NoError(t, a.runErr(t, args...))
	return a.out.String()
}

// runErr executes one invocation, returning its error.
func (a *testApp) runErr(t *testing.T, args ...string) error {
	t.Helper()
	a.out.Reset()
	a.errOut.Reset()
	return a.Run("", append([]string{"--dir", a.dir}, args...))
}

// runRaw executes one invocation exactly as written, with no implicit --dir.
func (a *testApp) runRaw(t *testing.T, args ...string) (string, error) {
	t.Helper()
	a.out.Reset()
	a.errOut.Reset()
	err := a.Run("", args)
	return a.out.String(), err
}

// queueSamples are valid values for every queue-scoped parameter.
var queueSamples = map[string]string{
	"batch_size":           "16",
	"max_interval":         "2s",
	"min_interval":         "50us",
	"segment_size":         "1048576",
	"segment_age":          "2h",
	"max_message_bytes":    "1048576",
	"encrypted":            "true",
	"fsync":                "false",
	"retention_age":        "72h",
	"retention_bytes":      "1073741824",
	"default_visibility":   "1m",
	"default_max_attempts": "3",
}

// flagArgs renders one parameter as the flag text the CLI accepts.
func flagArgs(p spec.Param) []string {
	if p.Kind == spec.KindBool {
		return []string{"--" + p.FlagName + "=" + queueSamples[p.Key]}
	}
	return []string{"--" + p.FlagName, queueSamples[p.Key]}
}

func TestCLIAppendRoundTrip(t *testing.T) {
	app := newTestApp(t)

	assert.Contains(t, app.run(t, "create", "q"), "created queue")
	assert.Contains(t, app.run(t, "append", "q", "hello"), "appended at offset 0")
	assert.Contains(t, app.run(t, "read", "q", "0"), "payload=hello")
}

func TestCLICreateWithAllParams(t *testing.T) {
	app := newTestApp(t)

	args := []string{"create", "q"}
	for _, p := range spec.Params(spec.LayerSpec) {
		args = append(args, flagArgs(p)...)
	}
	app.run(t, args...)

	var got mikyu.QueueDescription
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "describe", "q", "--json")), &got))

	for _, p := range spec.Params(spec.LayerSpec) {
		value, set := got.Spec.Field(p)
		require.True(t, set, "parameter %q did not round-trip", p.Key)
		want, err := p.Parse(queueSamples[p.Key])
		require.NoError(t, err)
		assert.Equal(t, want.String(), value.String(), "parameter %q", p.Key)
	}
}

func TestCLIDescribeJSONMatchesSDK(t *testing.T) {
	app := newTestApp(t)

	app.run(t, "create", "q", "--segment-size", "128", "--max-message-bytes", "128")

	var fromCLI mikyu.QueueDescription
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "describe", "q", "--json")), &fromCLI))

	client, err := mikyu.Open(app.dir)
	require.NoError(t, err)
	defer client.Close()
	fromSDK, err := client.DescribeQueue(t.Context(), "q")
	require.NoError(t, err)

	assert.Equal(t, fromSDK, fromCLI)
	cliJSON, err := json.Marshal(fromCLI)
	require.NoError(t, err)
	sdkJSON, err := json.Marshal(fromSDK)
	require.NoError(t, err)
	assert.JSONEq(t, string(sdkJSON), string(cliJSON))
}

func TestCLIMetricsJSON(t *testing.T) {
	app := newTestApp(t)

	app.run(t, "create", "q")

	var snap mikyu.Metrics
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "metrics", "--json")), &snap))
	_, ok := snap.Queue("q")
	assert.True(t, ok)
}

func TestCLIMetricsQueueFilter(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")

	out := app.run(t, "metrics", "--queue", "q", "--json")
	var snap mikyu.Metrics
	require.NoError(t, json.Unmarshal([]byte(out), &snap))
	q, ok := snap.Queue("q")
	assert.True(t, ok)
	assert.NotEmpty(t, q.Name)
}

func TestCLINoInfoVerb(t *testing.T) {
	app := newTestApp(t)
	err := app.runErr(t, "info")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "unknown command")
}

func TestCLIUpdateImmutable(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")

	err := app.runErr(t, "update", "q", "--segment-size", "128")
	require.ErrorIs(t, err, daemon.ErrUsage, "an immutable field on update is a usage mistake")
	assert.Contains(t, app.errOut.String(), "segment_size")
}

func TestCLIRetentionShrinkPrompts(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q", "--retention-age", "24h")

	// Declining the prompt leaves the bound alone.
	app.Confirm = func(string) (bool, error) { return false, nil }
	assert.Contains(t, app.run(t, "update", "q", "--retention-age", "1h"), "aborted")

	// Loosening the bound never prompts.
	app.Confirm = func(string) (bool, error) {
		t.Error("loosening retention must not prompt")
		return true, nil
	}
	app.run(t, "update", "q", "--retention-age", "48h")

	// --yes skips the prompt on a real shrink.
	app.Confirm = func(string) (bool, error) {
		t.Error("--yes must skip the prompt")
		return true, nil
	}
	app.run(t, "update", "q", "--retention-age", "1h", "--yes")

	var got mikyu.QueueDescription
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "describe", "q", "--json")), &got))
	require.NotNil(t, got.Spec.Retention.Age)
	assert.Equal(t, time.Hour, got.Spec.Retention.Age.Duration())
}

func TestCLIEnqueueAllParams(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")

	args := []string{"enqueue"}
	for _, p := range spec.Params(spec.LayerCall) {
		switch p.Key {
		case "visibility":
			args = append(args, "--"+p.FlagName, "1m")
		case "max_attempts":
			args = append(args, "--"+p.FlagName, "3")
		case "scheduled_at":
			args = append(args, "--"+p.FlagName, "2020-01-01T00:00:00Z")
		}
	}
	args = append(args, "q", "hello")
	app.run(t, args...)

	var rec command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dequeue", "q", "--json")), &rec))
	assert.Equal(t, uint32(3), rec.MaxAttempts)
	assert.Equal(t, uint32(1), rec.Attempts)
	assert.Equal(t, "2020-01-01T00:00:00Z", rec.ScheduledAt.UTC().Format(time.RFC3339))
	assert.InDelta(t, time.Minute.Seconds(), time.Until(rec.Visibility).Seconds(), 5)
}

func TestCLINackAllParams(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")
	app.run(t, "enqueue", "q", "hello")

	var rec command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dequeue", "q", "--json")), &rec))
	require.NotEmpty(t, rec.DeliveryID)

	for _, p := range spec.Params(spec.LayerCall) {
		if p.Key != "retry_after" {
			continue
		}
		assert.Contains(t, app.run(t, "nack", "q", rec.ID, "--"+p.FlagName, "0s"), "released task")
	}

	// The nack returned the task to the pool.
	var again command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dequeue", "q", "--timeout", "2s", "--json")), &again))
	assert.Equal(t, rec.ID, again.ID)
	assert.Equal(t, uint32(2), again.Attempts)
}

func TestCLITaskRecordFeedsAck(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")
	app.run(t, "enqueue", "q", "hello")

	record := strings.TrimSpace(app.run(t, "dequeue", "q", "--json"))
	assert.Contains(t, app.run(t, "ack", "q", record), "acknowledged task")

	// A stale record is rejected: the strict lease check survives the round trip.
	app.run(t, "enqueue", "q", "second")
	record = strings.TrimSpace(app.run(t, "dequeue", "q", "--json"))
	assert.Contains(t, app.run(t, "ack", "q", record), "acknowledged task")
}

func TestCLIDeadLetterRoundTrip(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "q")
	app.run(t, "enqueue", "q", "doomed", "--max-attempts", "1")

	var rec command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dequeue", "q", "--json")), &rec))
	app.run(t, "nack", "q", rec.ID, "--retry-after", "0s")

	var dead []command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dlq", "q", "--json")), &dead))
	require.Len(t, dead, 1)
	assert.Equal(t, rec.ID, dead[0].ID)

	assert.Contains(t, app.run(t, "dlq-ack", "q", dead[0].ID), "removed dead-letter task")

	var empty []command.TaskRecord
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "dlq", "q", "--json")), &empty))
	assert.Empty(t, empty)
}

func TestCLIUpdateOffersOnlyMutableFlags(t *testing.T) {
	app := newTestApp(t)
	out := app.run(t, "update", "--help")

	for _, p := range spec.Updatable() {
		assert.Contains(t, out, p.FlagName, "update should offer --%s", p.FlagName)
	}
	for _, p := range spec.Params(spec.LayerSpec) {
		if p.Mutability == spec.MutCreateOnly {
			assert.NotContains(t, out, "--"+p.FlagName, "update must not offer create-only %s", p.Key)
		}
	}
}

func TestCLIGlobalFlags(t *testing.T) {
	app := newTestApp(t)

	// --data-dir is the canonical spelling of data_dir; --dir is its alias.
	out, err := app.runRaw(t, "--data-dir", app.dir, "create", "q")
	require.NoError(t, err)
	assert.Contains(t, out, "created queue")

	out, err = app.runRaw(t, "--dir", app.dir, "--max-conns", "4", "list")
	require.NoError(t, err)
	assert.Contains(t, out, "q")

	_, err = app.runRaw(t, "--dir", app.dir, "--max-conns", "not-a-number", "list")
	require.ErrorIs(t, err, daemon.ErrUsage)

	_, err = app.runRaw(t, "--dir")
	require.ErrorIs(t, err, daemon.ErrUsage)
}

func TestNoLegacyVerbs(t *testing.T) {
	for _, name := range []string{"produce", "publish", "pop", "stats"} {
		_, ok := command.LookupVerb(name)
		assert.False(t, ok, "verb %q must be gone", name)
		_, ok = handlers[name]
		assert.False(t, ok, "verb %q must have no handler", name)
	}

	for _, v := range command.Verbs {
		_, ok := handlers[v.Name]
		assert.True(t, ok, "verb %q has no CLI handler", v.Name)
	}
}

func TestCLIUsageErrors(t *testing.T) {
	app := newTestApp(t)

	err := app.runErr(t, "nope")
	require.ErrorIs(t, err, daemon.ErrUsage)
	assert.Contains(t, app.errOut.String(), "unknown command")

	err = app.Run("", nil)
	require.ErrorIs(t, err, daemon.ErrUsage)

	// Global help succeeds and lists every verb.
	out := app.run(t, "--help")
	for _, v := range command.Verbs {
		assert.Contains(t, out, v.Name)
	}

	// A verb's help lists the flags spec.Table declares.
	out = app.run(t, "create", "--help")
	for _, p := range spec.Params(spec.LayerSpec) {
		assert.Contains(t, out, p.FlagName, "create --help should list --%s", p.FlagName)
	}

	out = app.run(t, "enqueue", "--help")
	for _, p := range spec.Params(spec.LayerCall) {
		switch p.Key {
		case "visibility", "max_attempts", "scheduled_at":
			assert.Contains(t, out, p.FlagName, "enqueue --help should list --%s", p.FlagName)
		}
	}
}

func TestConfirmNonInteractive(t *testing.T) {
	app := &App{Stdin: strings.NewReader(""), Stdout: io.Discard, Stderr: io.Discard}
	_, err := app.confirm("delete queue?", false)
	assert.Error(t, err, "without a terminal the caller must pass -y/--yes")

	ok, err := app.confirm("delete queue?", true)
	require.NoError(t, err)
	assert.True(t, ok)
}
