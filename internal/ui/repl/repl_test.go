package repl

import (
	"testing"

	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
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
