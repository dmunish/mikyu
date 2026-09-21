package command_test

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var update = flag.Bool("update", false, "rewrite golden files")

func TestIndexRendersInEveryMode(t *testing.T) {
	for _, mode := range []command.Mode{command.ModeCLI, command.ModeREPL} {
		var buf bytes.Buffer
		require.NoError(t, command.WriteIndex(&buf, command.RenderOptions{Mode: mode, Color: false}))
		assert.NotEmpty(t, buf.String())
	}
}

func TestEveryVerbRenders(t *testing.T) {
	for _, v := range command.Verbs {
		v := v
		t.Run(v.Name, func(t *testing.T) {
			var buf bytes.Buffer
			require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
			out := buf.String()
			assert.Contains(t, out, v.Name)
			assert.Contains(t, out, v.Summary)
		})
	}
}

func TestSummariesAreOneLine(t *testing.T) {
	for _, v := range command.Verbs {
		assert.NotEmpty(t, v.Summary, "%s has no summary", v.Name)
		assert.LessOrEqual(t, len(v.Summary), 60, "%s summary is longer than 60 chars", v.Name)
		assert.False(t, strings.HasSuffix(v.Summary, "."), "%s summary ends with a period", v.Name)
		assert.Equal(t, strings.ToUpper(v.Summary[:1]), v.Summary[:1],
			"%s summary must start with a capital", v.Name)
	}
}

func TestDispatchableVerbsHaveLongAndUsage(t *testing.T) {
	for _, v := range command.Verbs {
		if v.Intercepted {
			continue
		}
		assert.NotEmpty(t, v.Long, "%s has no long description", v.Name)
		assert.NotEmpty(t, v.Usage, "%s has no usage lines", v.Name)
	}
}

func TestEverySpecParamHasDoc(t *testing.T) {
	for _, layer := range []spec.Layer{spec.LayerSpec, spec.LayerSession, spec.LayerCall} {
		for _, p := range spec.Params(layer) {
			assert.NotEmpty(t, p.Doc, "%s has no doc", p.Key)
		}
	}
}

func TestCreateHelpListsEverySpecFlag(t *testing.T) {
	v, ok := command.LookupVerb("create")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	for _, p := range spec.Params(spec.LayerSpec) {
		assert.Contains(t, out, "--"+p.FlagName, "create detail is missing --%s", p.FlagName)
		assert.Contains(t, out, p.Doc, "create detail is missing the doc for %s", p.Key)
	}
}

func TestUpdateHelpOmitsCreateOnlyFlags(t *testing.T) {
	v, ok := command.LookupVerb("update")
	require.True(t, ok)

	var buf bytes.Buffer
	require.NoError(t, command.WriteVerb(&buf, v, command.RenderOptions{Mode: command.ModeCLI, Color: false}))
	out := buf.String()

	for _, p := range spec.Updatable() {
		assert.Contains(t, out, "--"+p.FlagName, "update detail is missing --%s", p.FlagName)
	}
	for _, p := range spec.Params(spec.LayerSpec) {
		if p.Mutability == spec.MutCreateOnly {
			assert.NotContains(t, out, "--"+p.FlagName,
				"update detail must not list create-only --%s", p.FlagName)
		}
	}
}

func TestIndexHasNoFlags(t *testing.T) {
	for _, mode := range []command.Mode{command.ModeCLI, command.ModeREPL} {
		var buf bytes.Buffer
		require.NoError(t, command.WriteIndex(&buf, command.RenderOptions{
			Mode: mode, Color: false,
		}))
		for _, line := range strings.Split(buf.String(), "\n") {
			trimmed := strings.TrimLeft(line, " ")
			assert.False(t, strings.HasPrefix(trimmed, "--"),
				"the index must not render flag rows (mode %d): %q", mode, line)
		}
	}
}

func TestIndexFiltersByMode(t *testing.T) {
	var cli, repl bytes.Buffer
	require.NoError(t, command.WriteIndex(&cli, command.RenderOptions{
		Mode: command.ModeCLI, Color: false,
	}))
	require.NoError(t, command.WriteIndex(&repl, command.RenderOptions{
		Mode: command.ModeREPL, Color: false,
	}))

	// REPL-only session verbs are absent from the CLI index and present in the REPL index.
	assert.False(t, indexHasVerb(cli.String(), "use"))
	assert.True(t, indexHasVerb(repl.String(), "use"))
	assert.False(t, indexHasVerb(cli.String(), "exit"))
	assert.True(t, indexHasVerb(repl.String(), "exit"))

	// The footer hint differs by mode.
	assert.Contains(t, cli.String(), "mikyu <verb> --help")
	assert.NotContains(t, repl.String(), "mikyu <verb> --help")
}

func TestLookupRejectsLegacyNames(t *testing.T) {
	for _, name := range []string{"produce", "publish", "pop", "stats", "info"} {
		_, ok := command.LookupVerb(name)
		assert.False(t, ok, "legacy name %q must not resolve", name)
	}
}

func TestLookupResolvesAlias(t *testing.T) {
	v, ok := command.LookupVerb("quit")
	require.True(t, ok)
	assert.Equal(t, "exit", v.Name)
	assert.Contains(t, v.Aliases, "quit")
}

func TestAliasesDoNotShadowRealVerbs(t *testing.T) {
	names := map[string]bool{}
	for _, v := range command.Verbs {
		names[v.Name] = true
	}
	for _, v := range command.Verbs {
		for _, a := range v.Aliases {
			assert.False(t, names[a], "alias %q shadows the real verb %q", a, a)
		}
	}
}

// TestGoldenHelpPages captures the pages users read most often.
// Run `go test ./internal/ui/command -run TestGolden -update` once to create them.
func TestGoldenHelpPages(t *testing.T) {
	cases := []struct {
		name string
		verb string
		mode command.Mode
	}{
		{"index_cli", "", command.ModeCLI},
		{"index_repl", "", command.ModeREPL},
		{"create_cli", "create", command.ModeCLI},
		{"enqueue_cli", "enqueue", command.ModeCLI},
		{"update_cli", "update", command.ModeCLI},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			opts := command.RenderOptions{Mode: tc.mode, Color: false, Width: 80}
			var buf bytes.Buffer
			var err error
			if tc.verb == "" {
				err = command.WriteIndex(&buf, opts)
			} else {
				v, ok := command.LookupVerb(tc.verb)
				require.True(t, ok, "unknown verb %q", tc.verb)
				err = command.WriteVerb(&buf, v, opts)
			}
			require.NoError(t, err)
			golden(t, tc.name+".txt", buf.Bytes())
		})
	}
}

func golden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(path, got, 0o644))
		return
	}
	want, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		t.Skipf("golden %s is missing; run with -update to create it", path)
	}
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

// indexHasVerb reports whether name appears as the first token of a line.
func indexHasVerb(out, name string) bool {
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && fields[0] == name {
			return true
		}
	}
	return false
}
