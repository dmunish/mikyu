package daemon

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/dmunish/mikyu/internal/credentials"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// tokenFile writes a one-line token file and returns its path.
func tokenFile(tb testing.TB, token string) string {
	tb.Helper()
	path := filepath.Join(tb.TempDir(), "token")
	require.NoError(tb, os.WriteFile(path, []byte(token), 0o600))
	return path
}

func TestStatusReportsServing(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	finish := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))
	defer finish()

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStatus([]string{"--target", target}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "serving "+target)
	assert.Empty(t, stderr.String())
}

func TestStatusJSON(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	finish := startDaemon(t, target, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"))
	defer finish()

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStatus([]string{"--target", target, "--json"}, &stdout, &stderr))

	var view statusView
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &view))
	assert.Equal(t, target, view.Target)
	assert.NotNil(t, view.Queues)
}

func TestStatusDeadTarget(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStatus([]string{"--target", "tcp://127.0.0.1:1"}, &stdout, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no daemon serving tcp://127.0.0.1:1")
	assert.Contains(t, err.Error(), "mikyu daemon run")
}

func TestStatusUnauthorizedWithoutCredential(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	tok := tokenFile(t, "0123456789abcdef")
	shutdown := spec.Session{Auth: spec.AuthConfig{Method: "token", Token: "0123456789abcdef"}}
	finish := startDaemonWithShutdown(t, target, shutdown, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"),
		"--auth-method", "token", "--auth-token-file", tok)
	defer finish()

	var stdout, stderr bytes.Buffer
	err := runStatus([]string{"--target", target}, &stdout, &stderr)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unauthorized")
}

func TestStatusWithTokenFile(t *testing.T) {
	target := "tcp://" + freeTCPAddr(t)
	tok := tokenFile(t, "0123456789abcdef")
	shutdown := spec.Session{Auth: spec.AuthConfig{Method: "token", Token: "0123456789abcdef"}}
	finish := startDaemonWithShutdown(t, target, shutdown, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"),
		"--auth-method", "token", "--auth-token-file", tok)
	defer finish()

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStatus([]string{"--target", target, "--auth-token-file", tok}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "serving "+target)
}

func TestStatusUsesStoredCredential(t *testing.T) {
	dir := t.TempDir()
	restore := credentials.TestUserConfigDir(func() (string, error) { return dir, nil })
	defer restore()

	target := "tcp://" + freeTCPAddr(t)
	tok := tokenFile(t, "0123456789abcdef")
	shutdown := spec.Session{Auth: spec.AuthConfig{Method: "token", Token: "0123456789abcdef"}}
	finish := startDaemonWithShutdown(t, target, shutdown, "--target", target, "--data-dir", filepath.Join(t.TempDir(), "data"),
		"--auth-method", "token", "--auth-token-file", tok)
	defer finish()

	require.NoError(t, credentials.Save(target, "0123456789abcdef"))

	var stdout, stderr bytes.Buffer
	require.NoError(t, runStatus([]string{"--target", target}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "serving "+target)
}

func TestStatusRejectsAuthTokenFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStatus([]string{"--auth-token", "0123456789abcdef"}, &stdout, &stderr)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUsage)
	assert.Contains(t, stderr.String(), "rejected")
}

func TestStatusHelpPrintsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	err := runStatus([]string{"--help"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Contains(t, stdout.String(), "Usage: mikyu daemon status [flags]")
	assert.Contains(t, stdout.String(), "--auth-token-file")
}

func TestClientSessionEnvAndDefaults(t *testing.T) {
	target := "tcp://127.0.0.1:9999"
	t.Setenv("MIKYU_TARGET", target)
	t.Setenv("MIKYU_TLS_ENABLED", "true")

	var stdout, stderr bytes.Buffer
	sess, err := clientSession("mikyu daemon status", nil, &stdout, &stderr)
	require.NoError(t, err)
	assert.Equal(t, target, sess.Target)
	assert.True(t, sess.TLS.Enabled)
}

func TestClientSessionFlagOverridesEnv(t *testing.T) {
	t.Setenv("MIKYU_TARGET", "tcp://127.0.0.1:9999")

	var stdout, stderr bytes.Buffer
	sess, err := clientSession("mikyu daemon status", []string{"--target", "tcp://10.0.0.1:7000"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Equal(t, "tcp://10.0.0.1:7000", sess.Target)
}

func TestClientSessionStoredCredentialIsLastLayer(t *testing.T) {
	dir := t.TempDir()
	restore := credentials.TestUserConfigDir(func() (string, error) { return dir, nil })
	defer restore()

	target := "tcp://127.0.0.1:9999"
	require.NoError(t, credentials.Save(target, "stored-token-value"))

	var stdout, stderr bytes.Buffer
	sess, err := clientSession("mikyu daemon status", []string{"--target", target}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Equal(t, "stored-token-value", sess.Auth.Token)

	sess, err = clientSession("mikyu daemon status", []string{"--target", target, "--auth-token-file", "flag.txt"}, &stdout, &stderr)
	require.NoError(t, err)
	assert.Empty(t, sess.Auth.Token, "an explicit token file wins over the stored credential")
	assert.Equal(t, "flag.txt", sess.Auth.TokenFile)
}

func TestClientSessionUnknownFlagIsUsage(t *testing.T) {
	var stdout, stderr bytes.Buffer
	_, err := clientSession("mikyu daemon status", []string{"--bogus"}, &stdout, &stderr)
	require.Error(t, err)
	assert.True(t, errors.Is(err, ErrUsage) || errors.Is(err, flag.ErrHelp))
}
