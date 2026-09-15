package spec_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/config"
	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/spec"
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
		return "1048576"
	case spec.KindBool:
		return "true"
	case spec.KindString:
		if p.Key == "target" {
			return "tcp://127.0.0.1:7420"
		}
		if p.Key == "metrics_listen" {
			return "127.0.0.1:9090"
		}
		return "sample"
	case spec.KindTime:
		return "2020-01-01T00:00:00Z"
	default:
		return "sample"
	}
}

// TestParitySurfaces checks that every spec.Table parameter is reachable from
// every surface its layer allows: SDK option, CLI flag, env, yaml, queues.json,
// and the wire.
func TestParitySurfaces(t *testing.T) {
	root := moduleRoot(t)
	clientFuncs := exportedFuncs(t, filepath.Join(root, "internal", "client"))
	taskFuncs := exportedFuncs(t, filepath.Join(root, "internal", "queue", "taskqueue"))
	facadeFuncs := exportedFuncs(t, root)
	daemonFlags := daemonFlagNames(t, filepath.Join(root, "internal", "daemon", "daemon.go"))

	var sess spec.Session
	globalFlags := command.SessionFlags(&sess, nil)

	var queueSpec spec.Spec
	createFlags := command.QueueFlagSet("create", &queueSpec)
	patchFlags := command.QueuePatchFlagSet("update", &queueSpec)

	var enqueueOpts command.EnqueueOptions
	enqueueFlags := command.EnqueueFlagSet("enqueue", &enqueueOpts)
	var retryAfter time.Duration
	var explicit bool
	nackFlags := command.NackFlagSet("nack", &retryAfter, &explicit)

	for _, p := range spec.Params(spec.LayerSpec) {
		t.Run("spec/"+p.Key, func(t *testing.T) {
			assert.Contains(t, clientFuncs, p.OptionName, "client SDK option missing")
			assert.Contains(t, facadeFuncs, p.OptionName, "facade option missing")
			assert.NotNil(t, createFlags.Lookup(p.FlagName), "create flag missing")
			if p.Mutability == spec.MutMutable {
				assert.NotNil(t, patchFlags.Lookup(p.FlagName), "update flag missing for a mutable parameter")
			} else {
				assert.Nil(t, patchFlags.Lookup(p.FlagName), "create-only parameter must not appear on update")
			}
			require.NotNil(t, globalFlags.Lookup("dir"))
			assertEnvSpec(t, p)
			assertYAMLSpec(t, p)
		})
	}

	for _, p := range spec.Params(spec.LayerCall) {
		t.Run("call/"+p.Key, func(t *testing.T) {
			assert.Contains(t, taskFuncs, p.OptionName, "taskqueue option missing")
			assert.Contains(t, facadeFuncs, p.OptionName, "facade option missing")
			if p.Key == "retry_after" {
				assert.NotNil(t, nackFlags.Lookup(p.FlagName), "nack flag missing")
				return
			}
			assert.NotNil(t, enqueueFlags.Lookup(p.FlagName), "enqueue flag missing")
		})
	}

	for _, p := range spec.Params(spec.LayerSession) {
		t.Run("session/"+p.Key, func(t *testing.T) {
			assert.Contains(t, clientFuncs, p.OptionName, "client SDK option missing")
			assert.Contains(t, facadeFuncs, p.OptionName, "facade option missing")
			assertEnvSession(t, p)
			assertYAMLSession(t, p)

			if p.Scope != spec.ScopeServer {
				assert.NotNil(t, globalFlags.Lookup(p.FlagName), "client-scope flag missing")
			}
			if p.Scope != spec.ScopeClient {
				assert.True(t, daemonFlags[p.FlagName], "mikyu daemon flag missing")
			}
		})
	}

	t.Run("queues.json", func(t *testing.T) { assertQueuesJSON(t) })
	t.Run("wire", func(t *testing.T) { assertWireSchema(t) })
}

// assertEnvSpec sets one parameter through the environment of the real loader.
func assertEnvSpec(t *testing.T, p spec.Param) {
	t.Helper()
	sample := sampleText(p)
	t.Setenv(p.EnvName, sample)

	cfg := loadConfig(t, "")
	want, err := p.Parse(sample)
	require.NoError(t, err)
	got, set := cfg.Defaults.Field(p)
	require.True(t, set, "%s did not reach the session defaults", p.EnvName)
	assert.Equal(t, want.String(), got.String())
}

// assertYAMLSpec sets one parameter through a mikyu.yaml defaults block.
func assertYAMLSpec(t *testing.T, p spec.Param) {
	t.Helper()
	group, leaf, ok := strings.Cut(p.Path, ".")
	require.True(t, ok, "spec path %q is not nested", p.Path)

	cfg := loadConfig(t, "defaults:\n  "+group+":\n    "+leaf+": "+sampleText(p)+"\n")
	want, err := p.Parse(sampleText(p))
	require.NoError(t, err)
	got, set := cfg.Defaults.Field(p)
	require.True(t, set, "%s did not reach the session defaults", p.Path)
	assert.Equal(t, want.String(), got.String())
}

func assertEnvSession(t *testing.T, p spec.Param) {
	t.Helper()
	t.Setenv(p.EnvName, sampleText(p))
	cfg := loadConfig(t, "")
	assertSessionField(t, cfg.Server, p)
}

func assertYAMLSession(t *testing.T, p spec.Param) {
	t.Helper()
	cfg := loadConfig(t, "server:\n  "+p.Key+": "+sampleText(p)+"\n")
	assertSessionField(t, cfg.Server, p)
}

func assertSessionField(t *testing.T, sess spec.Session, p spec.Param) {
	t.Helper()
	want, err := p.Parse(sampleText(p))
	require.NoError(t, err)
	field := reflect.ValueOf(sess).FieldByName(sessionField(p.Key))
	require.True(t, field.IsValid(), "spec.Session has no %s field", sessionField(p.Key))
	assert.Equal(t, want.String(), fmt.Sprint(field.Interface()))
}

// sessionField maps a session key to its spec.Session field name.
func sessionField(key string) string {
	var b strings.Builder
	for word := range strings.SplitSeq(key, "_") {
		if word == "tls" {
			b.WriteString("TLS")
			continue
		}
		b.WriteString(strings.ToUpper(word[:1]) + word[1:])
	}
	return b.String()
}

// assertQueuesJSON checks queues.json stores every parameter the register write gives it.
func assertQueuesJSON(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	n, release, err := node.Acquire(dir, spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()

	full := fullSpec(t)
	require.NoError(t, n.CreateQueue(t.Context(), "q", full))

	data, err := os.ReadFile(filepath.Join(dir, "queues.json"))
	require.NoError(t, err)

	var doc struct {
		Queues map[string]map[string]map[string]json.RawMessage `json:"queues"`
	}
	require.NoError(t, json.Unmarshal(data, &doc))
	stored := doc.Queues["q"]
	require.NotNil(t, stored)

	for _, p := range spec.Params(spec.LayerSpec) {
		group, leaf, _ := strings.Cut(p.Path, ".")
		_, ok := stored[group][leaf]
		assert.True(t, ok, "queues.json is missing %s", p.Path)
	}
}

// assertWireSchema checks the create/update JSON codec carries every parameter.
func assertWireSchema(t *testing.T) {
	t.Helper()
	full := fullSpec(t)

	encoded, err := spec.EncodeSpec(full)
	require.NoError(t, err)

	for _, p := range spec.Params(spec.LayerSpec) {
		group, leaf, _ := strings.Cut(p.Path, ".")
		var groups map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(encoded, &groups))
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(groups[group], &fields))
		assert.Contains(t, fields, leaf, "wire JSON is missing %s", p.Path)
	}

	decoded, err := spec.DecodeSpec(encoded)
	require.NoError(t, err)
	assert.True(t, reflect.DeepEqual(full, decoded), "the wire codec changed the spec")
}

// fullSpec is every queue-scoped parameter set to its sample value.
func fullSpec(t *testing.T) spec.Spec {
	t.Helper()
	var s spec.Spec
	for _, p := range spec.Params(spec.LayerSpec) {
		v, err := p.Parse(sampleText(p))
		require.NoError(t, err)
		p.Apply(&s, v)
	}
	return s
}

// loadConfig writes an optional mikyu.yaml and loads it.
func loadConfig(t *testing.T, body string) *config.Config {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "mikyu.yaml")
	if body == "" {
		body = "server:\n  data_dir: " + dir + "\n"
	}
	require.NoError(t, os.WriteFile(path, []byte(body), 0o644))

	cfg, _, err := config.Load(path)
	require.NoError(t, err)
	return cfg
}

// exportedFuncs lists the exported package-level functions of dir's package.
func exportedFuncs(t *testing.T, dir string) []string {
	t.Helper()
	fset := token.NewFileSet()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)

	var names []string
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, filepath.Join(dir, e.Name()), nil, parser.SkipObjectResolution)
		require.NoError(t, err)
		for _, decl := range f.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && fn.Recv == nil && fn.Name.IsExported() {
				names = append(names, fn.Name.Name)
			}
		}
	}
	return names
}

var daemonFlagPattern = regexp.MustCompile(`fs\.(?:String|Bool|Duration|Int|Int64|Uint)\("([a-z0-9-]+)"`)

// daemonFlagNames reads the flags mikyu daemon registers.
func daemonFlagNames(t *testing.T, path string) map[string]bool {
	t.Helper()
	src, err := os.ReadFile(path)
	require.NoError(t, err)

	names := map[string]bool{}
	for _, m := range daemonFlagPattern.FindAllStringSubmatch(string(src), -1) {
		names[m[1]] = true
	}
	return names
}

// TestSpecTagsIdentical checks every spec field carries matching json and yaml
// keys, so mikyu.yaml and queues.json read the same schema.
func TestSpecTagsIdentical(t *testing.T) {
	checkTags(t, reflect.TypeFor[spec.Spec]())
}

func checkTags(t *testing.T, rt reflect.Type) {
	t.Helper()
	for field := range rt.Fields() {
		jsonTag, jsonOK := field.Tag.Lookup("json")
		yamlTag, yamlOK := field.Tag.Lookup("yaml")
		require.True(t, jsonOK, "%s.%s has no json tag", rt.Name(), field.Name)
		require.True(t, yamlOK, "%s.%s has no yaml tag", rt.Name(), field.Name)

		jsonKey, jsonOmit := partitionTag(jsonTag)
		yamlKey, yamlOmit := partitionTag(yamlTag)
		assert.Equal(t, jsonKey, yamlKey, "%s.%s key mismatch", rt.Name(), field.Name)
		assert.Equal(t, jsonOmit, yamlOmit, "%s.%s omit directive mismatch", rt.Name(), field.Name)

		if field.Type.Kind() == reflect.Struct {
			checkTags(t, field.Type)
		}
	}
}

// partitionTag splits a struct tag into its key and whether it omits empties.
func partitionTag(tag string) (string, bool) {
	key, rest, _ := strings.Cut(tag, ",")
	return key, strings.Contains(rest, "omit")
}

// moduleRoot walks up from the test's working directory to the module root.
func moduleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, parent, dir, "no go.mod above %s", dir)
		dir = parent
	}
}
