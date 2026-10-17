// Package credentials stores per-target bearer tokens in the user's config
// directory, written atomically at mode 0600.
package credentials

import (
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

const credentialsFileName = "credentials"

// userConfigDir resolves the user config directory; tests override it.
var userConfigDir = os.UserConfigDir

// TestUserConfigDir replaces the config-dir resolver for the duration of a
// test and returns a restore function.
func TestUserConfigDir(fn func() (string, error)) func() {
	old := userConfigDir
	userConfigDir = fn
	return func() { userConfigDir = old }
}

// Path returns the absolute path for the user's credential store.
func Path() (string, error) {
	dir, err := userConfigDir()
	if err != nil {
		return "", err
	}
	mikyuDir := filepath.Join(dir, "mikyu")
	if err := os.MkdirAll(mikyuDir, 0o700); err != nil {
		return "", err
	}
	return filepath.Join(mikyuDir, credentialsFileName), nil
}

func read(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return map[string]string{}, nil
		}
		return nil, err
	}
	var m map[string]string
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	if m == nil {
		return map[string]string{}, nil
	}
	return m, nil
}

func write(path string, m map[string]string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	data, err := yaml.Marshal(m)
	if err != nil {
		return err
	}
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0o600)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// Load reads the stored token for target, or ("", false) when none is stored.
func Load(target string) (string, bool) {
	path, err := Path()
	if err != nil {
		return "", false
	}
	m, err := read(path)
	if err != nil {
		return "", false
	}
	tok, ok := m[target]
	return tok, ok
}

// Save writes target: token atomically at 0600.
func Save(target, token string) error {
	path, err := Path()
	if err != nil {
		return err
	}
	m, err := read(path)
	if err != nil {
		return err
	}
	m[target] = token
	return write(path, m)
}

// Delete removes the entry for target; a missing entry is a no-op.
func Delete(target string) error {
	path, err := Path()
	if err != nil {
		return err
	}
	m, err := read(path)
	if err != nil {
		return err
	}
	delete(m, target)
	return write(path, m)
}
