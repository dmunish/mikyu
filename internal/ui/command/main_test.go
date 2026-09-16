package command_test

import (
	"os"
	"testing"
)

// TestMain redirects os.UserConfigDir() into a per-process temp directory
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "mikyu-command-test-*")
	if err != nil {
		panic(err)
	}
	os.Setenv("XDG_CONFIG_HOME", dir)
	os.Setenv("HOME", dir)
	os.Setenv("AppData", dir)
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}
