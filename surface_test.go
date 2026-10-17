package mikyu_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/cli"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sampleText is a valid value for every parameter kind.
func sampleText(p spec.Param) string {
	switch p.Kind {
	case spec.KindInt:
		return "16"
	case spec.KindUint32:
		return "3"
	case spec.KindDuration:
		return "2s"
	case spec.KindBytes:
		return byteSample(p.Key)
	case spec.KindBool:
		return "true"
	case spec.KindEnum:
		return p.Allowed[0]
	default:
		return stringSample(p.Key)
	}
}

// byteSample keeps the explicit pair rules satisfied (payload fits a segment, cache covers it).
func byteSample(key string) string {
	switch key {
	case "max_segment":
		return "67108864"
	case "max_payload":
		return "1048576"
	case "max_cache":
		return "33554432"
	default:
		return "1048576"
	}
}

func stringSample(key string) string {
	switch key {
	case "target":
		return "tcp://127.0.0.1:7420"
	case "metrics_listen":
		return "127.0.0.1:9090"
	case "auth_method":
		return "none"
	case "auth_token":
		return "0123456789abcdef"
	default:
		return "sample"
	}
}

// fullSpec is every queue-scoped parameter set to its sample value.
func fullSpec(t *testing.T) mikyu.Spec {
	t.Helper()
	var s mikyu.Spec
	for _, p := range spec.Params(spec.LayerSpec) {
		v, err := p.Parse(sampleText(p))
		require.NoError(t, err)
		p.Apply(&s, v)
	}
	return s
}

// specWith is a spec holding one parameter at its sample value.
func specWith(t *testing.T, p spec.Param) mikyu.Spec {
	t.Helper()
	var s mikyu.Spec
	v, err := p.Parse(sampleText(p))
	require.NoError(t, err)
	p.Apply(&s, v)
	return s
}

// defaultsDoc renders every queue-scoped parameter into a mikyu.yaml defaults block.
func defaultsDoc(t *testing.T) string {
	t.Helper()
	groups := map[string][]string{}
	for _, p := range spec.Params(spec.LayerSpec) {
		group, leaf, _ := strings.Cut(p.Path, ".")
		groups[group] = append(groups[group], "    "+leaf+": "+sampleText(p))
	}

	var b strings.Builder
	b.WriteString("defaults:\n")
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		b.WriteString("  ")
		b.WriteString(group)
		b.WriteString(":\n")
		b.WriteString(strings.Join(groups[group], "\n"))
		b.WriteString("\n")
	}
	return b.String()
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// startDaemon runs the daemon in-process until the returned stop sends shutdown.
func startDaemon(t *testing.T, target, configPath, dataDir string) func() {
	t.Helper()
	exited := make(chan error, 1)
	go func() {
		exited <- daemon.Run([]string{"--config", configPath, "--target", target, "--data-dir", dataDir}, io.Discard)
	}()
	require.Eventually(t, func() bool { return daemon.IsDaemonAlive(target) }, 10*time.Second, 25*time.Millisecond,
		"daemon never came online")

	stopped := false
	stop := func() {
		if stopped {
			return
		}
		stopped = true
		require.NoError(t, daemon.SendShutdownSignal(target, spec.BuiltinSession()))
		select {
		case err := <-exited:
			assert.NoError(t, err)
		case <-time.After(10 * time.Second):
			t.Error("daemon did not stop after the shutdown signal")
		}
	}
	t.Cleanup(stop)
	return stop
}

// cliRunner drives the CLI in-process with captured output.
type cliRunner struct {
	app *cli.App
	out *bytes.Buffer
}

func newCLIRunner() *cliRunner {
	out := &bytes.Buffer{}
	return &cliRunner{app: &cli.App{Stdin: strings.NewReader(""), Stdout: out, Stderr: out}, out: out}
}

func (c *cliRunner) run(t *testing.T, args ...string) string {
	t.Helper()
	c.out.Reset()
	require.NoError(t, c.app.Run("", args))
	return c.out.String()
}

// TestParityEndToEnd drives every queue-scoped parameter through the embedded
// SDK, the remote SDK, the CLI's --dir mode, and mikyu.yaml defaults, and
// requires all four to resolve to the same effective spec.
func TestParityEndToEnd(t *testing.T) {
	ctx := t.Context()

	all := fullSpec(t)
	for _, p := range spec.Params(spec.LayerSpec) {
		t.Setenv(p.EnvName, sampleText(p))
	}

	daemonDir := t.TempDir()
	cfgPath := filepath.Join(daemonDir, "mikyu.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(defaultsDoc(t)), 0o644))

	target := "tcp://" + freePort(t)
	stop := startDaemon(t, target, cfgPath, filepath.Join(daemonDir, "data"))
	defer stop()

	embDir, cliDir := t.TempDir(), t.TempDir()
	cliRun := newCLIRunner()

	emb, err := mikyu.Open(embDir, mikyu.WithSessionSpec(all))
	require.NoError(t, err)
	rem, err := mikyu.Dial(target)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = emb.Close()
		_ = rem.Close()
	})

	for _, p := range spec.Params(spec.LayerSpec) {
		t.Run(p.Key, func(t *testing.T) {
			name := func(surface string) string { return surface + "-" + p.Key }
			opt := mikyu.QueueOption(func(s *mikyu.Spec) { *s = specWith(t, p) })

			_, err = emb.CreateLog(ctx, name("emb"), opt)
			require.NoError(t, err)
			_, err = rem.CreateLog(ctx, name("rem"), opt)
			require.NoError(t, err)
			_, err = rem.CreateLog(ctx, name("yml"))
			require.NoError(t, err)

			// Bool flags take their value with =, as the flag package requires.
			flagArgs := []string{"--" + p.FlagName, sampleText(p)}
			if p.Kind == spec.KindBool {
				flagArgs = []string{"--" + p.FlagName + "=" + sampleText(p)}
			}
			cliRun.run(t, append([]string{"--dir", cliDir, "create", "--type", "log", name("cli")}, flagArgs...)...)

			embDesc, err := emb.DescribeQueue(ctx, name("emb"))
			require.NoError(t, err)
			remDesc, err := rem.DescribeQueue(ctx, name("rem"))
			require.NoError(t, err)
			ymlDesc, err := rem.DescribeQueue(ctx, name("yml"))
			require.NoError(t, err)

			var cliDesc mikyu.QueueDescription
			raw := cliRun.run(t, "--dir", cliDir, "describe", name("cli"), "--json")
			require.NoError(t, json.Unmarshal([]byte(raw), &cliDesc))

			assert.Equal(t, effectiveJSON(t, embDesc), effectiveJSON(t, remDesc), "embedded vs remote")
			assert.Equal(t, effectiveJSON(t, embDesc), effectiveJSON(t, ymlDesc), "embedded vs yaml defaults")
			assert.Equal(t, effectiveJSON(t, embDesc), effectiveJSON(t, cliDesc), "embedded vs CLI")

			assert.Equal(t, embDesc.Defaults, remDesc.Defaults, "session defaults differ")
			assert.Equal(t, embDesc.Defaults, cliDesc.Defaults, "session defaults differ")

			want, err := p.Parse(sampleText(p))
			require.NoError(t, err)
			for _, d := range []mikyu.QueueDescription{embDesc, remDesc, cliDesc} {
				value, set := d.Spec.Field(p)
				require.True(t, set, "%s was not stored by an explicit surface", p.Key)
				assert.Equal(t, want.String(), value.String())
			}
			_, stored := ymlDesc.Spec.Field(p)
			assert.False(t, stored, "the defaults surface must inherit, not store")

			// Every surface survives a close and reopen unchanged.
			require.NoError(t, emb.Close())
			emb, err = mikyu.Open(embDir, mikyu.WithSessionSpec(all))
			require.NoError(t, err)
			embAfter, err := emb.DescribeQueue(ctx, name("emb"))
			require.NoError(t, err)
			assert.Equal(t, embDesc, embAfter, "embedded description changed across reopen")

			require.NoError(t, rem.Close())
			rem, err = mikyu.Dial(target)
			require.NoError(t, err)
			remAfter, err := rem.DescribeQueue(ctx, name("rem"))
			require.NoError(t, err)
			assert.Equal(t, remDesc, remAfter, "remote description changed across reopen")

			var cliAfter mikyu.QueueDescription
			raw = cliRun.run(t, "--dir", cliDir, "describe", name("cli"), "--json")
			require.NoError(t, json.Unmarshal([]byte(raw), &cliAfter))
			assert.Equal(t, cliDesc, cliAfter, "CLI description changed across reopen")
		})
	}
}

func effectiveJSON(t *testing.T, d mikyu.QueueDescription) string {
	t.Helper()
	data, err := json.Marshal(d.Effective)
	require.NoError(t, err)
	return string(data)
}

// TestNamingLaw holds the CLI verbs, the public methods, and the option
// constructors to the vocabulary of spec.Table.
func TestNamingLaw(t *testing.T) {
	bannedParts := []string{"Produce", "Publish", "Pop", "Stats", "Info", "TruncateQueue", "FetchOffset", "OriginalOffset", "DequeueNow",
		"Visibility", "BatchSize", "SegmentSize", "MaxMessageBytes", "MaxDLQ", "MaxConns", "Fsync", "Encrypted", "MinInterval", "MaxInterval"}

	for _, path := range command.VerbNames() {
		lower := strings.ToLower(path)
		for _, banned := range []string{"produce", "publish", "pop", "stats", "fetchoffset"} {
			assert.False(t, strings.Contains(lower, banned), "verb %q uses retired vocabulary", path)
		}
	}

	interfaces := []reflect.Type{
		reflect.TypeFor[mikyu.Client](),
		reflect.TypeFor[mikyu.LogQueue](),
		reflect.TypeFor[mikyu.TaskQueue](),
		reflect.TypeFor[mikyu.Group](),
	}
	for _, iface := range interfaces {
		for _, name := range methodNames(iface) {
			for _, banned := range bannedParts {
				assert.NotContains(t, name, banned, "%s.%s uses retired vocabulary", iface, name)
			}
		}
	}

	// The option constructors are exactly the table's option names, plus the
	// composition helpers that carry a whole session spec or disambiguate the
	// spec/call lease and max-attempts keys.
	compositionHelpers := map[string]bool{
		"WithSessionSpec":     true,
		"WithAutoStart":       true,
		"WithKey":             true,
		"WithRecordKey":       true,
		"WithTaskLease":       true,
		"WithTaskMaxAttempts": true,
	}
	have := map[string]bool{}
	for _, name := range exportedFuncs(t, ".") {
		if strings.HasPrefix(name, "With") {
			have[name] = true
		}
	}
	want := map[string]bool{}
	for _, p := range spec.Table {
		if p.OptionName != "" {
			want[p.OptionName] = true
		}
	}
	for _, name := range slices.Sorted(maps.Keys(want)) {
		assert.True(t, have[name], "option constructor %s is missing from the SDK", name)
	}
	for _, name := range slices.Sorted(maps.Keys(have)) {
		assert.True(t, want[name] || compositionHelpers[name], "unexpected option constructor %s", name)
	}
}

// TestNoIDSiblings checks the driver interfaces have one method per operation.
func TestNoIDSiblings(t *testing.T) {
	for _, iface := range driverInterfaces() {
		for _, name := range methodNames(iface) {
			assert.False(t, strings.HasSuffix(name, "ID"), "%s.%s is an ID sibling", iface, name)
		}
		for _, op := range []string{"Ack", "Nack", "Extend", "AckDeadLetter"} {
			_, ok := iface.MethodByName(op + "ID")
			assert.False(t, ok, "%s.%sID must not exist", iface, op)
		}
	}
}

// TestNoCloseOnDrivers checks driver lifetime belongs to the client, not the queue handle.
func TestNoCloseOnDrivers(t *testing.T) {
	for _, iface := range driverInterfaces() {
		_, ok := iface.MethodByName("Close")
		assert.False(t, ok, "%s must not expose Close", iface)
	}
}

// TestSurfaceNoInfo checks the Info and Stats surfaces are retired; Metrics is
// the only observation point on the client interfaces.
func TestSurfaceNoInfo(t *testing.T) {
	interfaces := []reflect.Type{
		reflect.TypeFor[mikyu.Client](),
		reflect.TypeFor[mikyu.LogQueue](),
		reflect.TypeFor[mikyu.TaskQueue](),
	}
	for _, iface := range interfaces {
		_, hasInfo := iface.MethodByName("Info")
		assert.False(t, hasInfo, "%s still exposes Info", iface)
		for _, name := range methodNames(iface) {
			assert.NotContains(t, name, "Info", "%s.%s uses the retired Info surface", iface, name)
			assert.NotContains(t, name, "Stats", "%s.%s uses the retired Stats surface", iface, name)
		}
	}
}

// TestMetricsIsOnlyStatsSurface pins the SDK's observation surface: the client
// exposes exactly one Metrics method and no retired Info/Stats methods at all.
func TestMetricsIsOnlyStatsSurface(t *testing.T) {
	iface := reflect.TypeFor[mikyu.Client]()
	metric, legacy := 0, 0
	for _, name := range methodNames(iface) {
		switch {
		case strings.Contains(name, "Metric"):
			metric++
		case strings.Contains(name, "Info"), strings.Contains(name, "Stats"):
			legacy++
		}
	}
	assert.Equal(t, 1, metric, "Client must expose exactly one Metric* method")
	assert.Zero(t, legacy, "Client still exposes a retired Info/Stats method")
}

func driverInterfaces() []reflect.Type {
	return []reflect.Type{
		reflect.TypeFor[mikyu.TaskQueue](),
		reflect.TypeFor[mikyu.LogQueue](),
	}
}

func methodNames(t reflect.Type) []string {
	if t.Kind() != reflect.Interface {
		t = reflect.PointerTo(t)
	}
	names := make([]string, 0, t.NumMethod())
	for method := range t.Methods() {
		names = append(names, method.Name)
	}
	return names
}

// TestSentinelUniqueness checks every Err* the facade exports is a distinct error.
func TestSentinelUniqueness(t *testing.T) {
	exported := map[string]error{
		"ErrImmutableField":          mikyu.ErrImmutableField,
		"ErrInvalidConfig":           mikyu.ErrInvalidConfig,
		"ErrSpecTooNew":              mikyu.ErrSpecTooNew,
		"ErrSpecTooLarge":            mikyu.ErrSpecTooLarge,
		"ErrLocked":                  mikyu.ErrLocked,
		"ErrRegistryCorrupt":         mikyu.ErrRegistryCorrupt,
		"ErrSessionDefaultsMismatch": mikyu.ErrSessionDefaultsMismatch,
		"ErrQueueExists":             mikyu.ErrQueueExists,
		"ErrQueueNotFound":           mikyu.ErrQueueNotFound,
		"ErrOffsetNotFound":          mikyu.ErrOffsetNotFound,
		"ErrOffsetTruncated":         mikyu.ErrOffsetTruncated,
		"ErrMessageTooLarge":         mikyu.ErrMessageTooLarge,
		"ErrQueueClosed":             mikyu.ErrQueueClosed,
		"ErrCorruptRecord":           mikyu.ErrCorruptRecord,
		"ErrTaskNotAvailable":        mikyu.ErrTaskNotAvailable,
		"ErrTaskNotFound":            mikyu.ErrTaskNotFound,
		"ErrTaskNotInflight":         mikyu.ErrTaskNotInflight,
		"ErrTaskStale":               mikyu.ErrTaskStale,
		"ErrTaskClosed":              mikyu.ErrTaskClosed,
		"ErrInvalidTask":             mikyu.ErrInvalidTask,
		"ErrCheckpointCorrupt":       mikyu.ErrCheckpointCorrupt,
		"ErrDeadLetterNotFound":      mikyu.ErrDeadLetterNotFound,
		"ErrDeadLetterStale":         mikyu.ErrDeadLetterStale,
		"ErrArchiveFailed":           mikyu.ErrArchiveFailed,
		"ErrNoData":                  mikyu.ErrNoData,
		"ErrRecordTooLarge":          mikyu.ErrRecordTooLarge,
		"ErrRecordCompacted":         mikyu.ErrRecordCompacted,
		"ErrDiskFull":                mikyu.ErrDiskFull,
	"ErrEncryptionKey":           mikyu.ErrEncryptionKey,
		"ErrWrongKind":               mikyu.ErrWrongKind,
		"ErrQueueFull":               mikyu.ErrQueueFull,
		"ErrProtocol":                mikyu.ErrProtocol,
		"ErrModeMismatch":            mikyu.ErrModeMismatch,
		"ErrTooManyConnections":      mikyu.ErrTooManyConnections,
		"ErrUnauthorized":            mikyu.ErrUnauthorized,
	}

	declared := exportedErrVars(t, ".")
	for _, name := range slices.Sorted(maps.Keys(declared)) {
		assert.Contains(t, exported, name, "%s is exported but not covered by the uniqueness check", name)
	}
	for _, name := range slices.Sorted(maps.Keys(exported)) {
		assert.True(t, declared[name], "%s is checked but not exported", name)
	}

	names := slices.Sorted(maps.Keys(exported))
	for i, a := range names {
		for _, b := range names[i+1:] {
			assert.False(t, errors.Is(exported[a], exported[b]), "%s and %s are errors.Is-equal", a, b)
			assert.NotEqual(t, exported[a].Error(), exported[b].Error(), "%s and %s share a message", a, b)
		}
	}
}

// exportedErrVars lists the Err* variables a package declares.
func exportedErrVars(t *testing.T, dir string) map[string]bool {
	t.Helper()
	src := readPackageSource(t, dir)
	re := regexp.MustCompile(`(?m)^\s*(Err[A-Za-z0-9]*)\s*=`)
	names := map[string]bool{}
	for _, m := range re.FindAllStringSubmatch(src, -1) {
		names[m[1]] = true
	}
	return names
}

// exportedFuncs lists the exported package-level functions a package declares.
func exportedFuncs(t *testing.T, dir string) []string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^func ([A-Z][A-Za-z0-9]*)\(`)
	var names []string
	for _, m := range re.FindAllStringSubmatch(readPackageSource(t, dir), -1) {
		names = append(names, m[1])
	}
	return names
}

func readPackageSource(t *testing.T, dir string) string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var b strings.Builder
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, err)
		b.Write(data)
		b.WriteString("\n")
	}
	return b.String()
}
