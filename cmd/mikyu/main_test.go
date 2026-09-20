package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersionOutput(t *testing.T) {
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	err = run([]string{"--version"})
	require.NoError(t, err)

	w.Close()
	os.Stdout = old

	var out bytes.Buffer
	_, _ = out.ReadFrom(r)

	assert.Contains(t, out.String(), "spec version")
}

func TestExitCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want int
	}{
		{"success", nil, 0},
		{"general error", errors.New("boom"), 1},
		{"wrapped general error", fmt.Errorf("open node: %w", errors.New("denied")), 1},
		{"usage error", fmt.Errorf("%w: flag provided but not defined: -nope", daemon.ErrUsage), 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, exitCode(tt.err))
		})
	}
}
