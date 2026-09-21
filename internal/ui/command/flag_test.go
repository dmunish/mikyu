package command_test

import (
	"testing"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/command"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseAnywhereAcceptsBothFlagForms covers the "--flag value" and
// "--flag=value" spellings side by side.
func TestParseAnywhereAcceptsBothFlagForms(t *testing.T) {
	var patch spec.Spec
	fs := command.QueueFlagSet("test", &patch)

	rest, err := command.ParseAnywhere(fs, []string{
		"queue_name",
		"--batch-size", "32",
		"--max-interval=5s",
	})
	require.NoError(t, err)
	assert.Equal(t, []string{"queue_name"}, rest)
	require.NotNil(t, patch.Engine.BatchSize)
	assert.Equal(t, 32, *patch.Engine.BatchSize)
	require.NotNil(t, patch.Engine.MaxInterval)
	assert.Equal(t, "5s", patch.Engine.MaxInterval.Duration().String())
}

// TestParseAnywhereRejectsImmutableFlag: an update-only parser flags a
// create-only parameter with a clear message.
func TestParseAnywhereRejectsImmutableFlag(t *testing.T) {
	var patch spec.Spec
	fs := command.QueuePatchFlagSet("update", &patch)

	_, err := command.ParseAnywhere(fs, []string{"--segment-size", "1024", "q"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "create-only")
}

func TestParseAnywhereAcceptsBareBoolFlag(t *testing.T) {
	var patch spec.Spec
	fs := command.QueueFlagSet("test", &patch)

	_, err := command.ParseAnywhere(fs, []string{"--fsync", "queue_name"})
	require.NoError(t, err)
	require.NotNil(t, patch.Engine.Fsync)
	assert.True(t, *patch.Engine.Fsync)
}

func TestHasHelpFlag(t *testing.T) {
	assert.True(t, command.HasHelpFlag([]string{"--help"}))
	assert.True(t, command.HasHelpFlag([]string{"-h"}))
	assert.True(t, command.HasHelpFlag([]string{"queue", "--help"}))
	assert.False(t, command.HasHelpFlag([]string{"--", "--help"}))
	assert.False(t, command.HasHelpFlag([]string{"queue"}))
}

// TestParseGlobalAcceptsBothFlagForms: leading session flags accept both
// spellings too.
func TestParseGlobalAcceptsBothFlagForms(t *testing.T) {
	g, rest, err := command.ParseGlobal("", []string{"--data-dir=/tmp/x", "--max-conns", "4", "list"})
	require.NoError(t, err)
	assert.Equal(t, "/tmp/x", g.Session.DataDir)
	assert.Equal(t, 4, g.Session.MaxConns)
	assert.Equal(t, []string{"list"}, rest)

	g, rest, err = command.ParseGlobal("", []string{"--dir", "/tmp/y", "list"})
	require.NoError(t, err)
	assert.Equal(t, "/tmp/y", g.Session.DataDir)
	assert.Equal(t, []string{"list"}, rest)
}
