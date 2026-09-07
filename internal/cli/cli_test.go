package cli

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPrintUsage(t *testing.T) {
	printUsage()
}

func TestConfirmCLINonInteractive(t *testing.T) {
	// confirmCLI should fail when stdout/stdin is non-interactive without -y flag
	ok, err := confirmCLI("delete queue?", []string{"queue-name"})
	assert.Error(t, err)
	assert.False(t, ok)

	// confirmCLI should succeed when -y or --yes flag is present
	ok, err = confirmCLI("delete queue?", []string{"queue-name", "-y"})
	assert.NoError(t, err)
	assert.True(t, ok)
}
