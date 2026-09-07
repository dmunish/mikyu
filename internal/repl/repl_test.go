package repl

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseREPLArgs(t *testing.T) {
	args := parseREPLArgs(`produce "hello world" @file.txt`)
	assert.Equal(t, []string{"produce", "hello world", "@file.txt"}, args)
}

func TestParseREPLArgsEmptyQuotes(t *testing.T) {
	args := parseREPLArgs(`produce "" @file`)
	assert.Equal(t, []string{"produce", "", "@file"}, args)
}

func TestFormatHelpArgs(t *testing.T) {
	formatted := formatHelpArgs("<queue> <msg>", 20)
	assert.NotEmpty(t, formatted)

	empty := formatHelpArgs("", 10)
	assert.Equal(t, 10, len(empty))
}
