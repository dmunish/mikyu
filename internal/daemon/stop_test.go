package daemon

import (
	"bytes"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/credentials"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStopDeadTarget(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStop(nil, []string{"--target", "tcp://127.0.0.1:1", "--yes"}, &stdout, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no daemon serving tcp://127.0.0.1:1")
}

func TestStopRefusesWithoutYesOnNonTerminal(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStop(nil, []string{"--target", "tcp://127.0.0.1:1"}, &stdout, &stderr)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, err.Error(), "--yes")
}

func TestStopCanceledByConfirmation(t *testing.T) {
	restore := confirmStop
	confirmStop = func(string) (bool, error) { return false, nil }
	t.Cleanup(func() { confirmStop = restore })

	var stdout, stderr bytes.Buffer
	err := runStop(nil, []string{"--target", "tcp://127.0.0.1:1"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "canceled")
}

func TestStopStopsTheDaemon(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	finish := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStop(nil, []string{"--target", target, "--yes"}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "stopped "+target)
	assert.Eventually(t, func() bool { return !IsDaemonAlive(target) }, 10*time.Second, 25*time.Millisecond,
		"the daemon must stop after mikyu daemon stop")
	finish()
}

func TestStopHonorsStoredCredential(t *testing.T) {
	dir := t.TempDir()
	restore := credentials.TestUserConfigDir(func() (string, error) { return dir, nil })
	t.Cleanup(restore)

	target := "tcp://" + freeTCPAddr(t)
	tok := tokenFile(t, "0123456789abcdef")
	shutdown := spec.Session{Auth: spec.AuthConfig{Method: "token", Token: "0123456789abcdef"}}
	finish := startDaemonWithShutdown(t, target, shutdown, "--target", target,
		"--data-dir", filepath.Join(t.TempDir(), "data"), "--auth-method", "token", "--auth-token-file", tok)

	require.NoError(t, credentials.Save(target, "0123456789abcdef"))
	t.Cleanup(func() { _ = credentials.Delete(target) })

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStop(nil, []string{"--target", target, "--yes"}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "stopped "+target)
	assert.Eventually(t, func() bool { return !IsDaemonAlive(target) }, 10*time.Second, 25*time.Millisecond,
		"the stored login credential must authorize the stop")
	finish()
}

func TestStopHelpPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStop(nil, []string{"--help"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "Usage: mikyu daemon stop [flags]")
	assert.Contains(t, stdout.String(), "-yes")
}
