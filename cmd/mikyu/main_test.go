package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/stretchr/testify/assert"
)

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
