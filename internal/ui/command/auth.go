package command

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/dmunish/mikyu"
	"github.com/goccy/go-yaml"
	"golang.org/x/term"
)

const credentialsFileName = "credentials"

// CredentialsPath returns the absolute path for the user's credential store.
func CredentialsPath() (string, error) {
	dir, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	mikyuDir := filepath.Join(dir, "mikyu")
	if err := os.MkdirAll(mikyuDir, 0o700); err != nil {
		return "", err
	}
	return filepath.Join(mikyuDir, credentialsFileName), nil
}

func readCredentials(path string) (map[string]string, error) {
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

func writeCredentials(path string, m map[string]string) error {
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

// LoadToken reads the stored token for the given target, or ("", false) if none.
func LoadToken(target string) (string, bool) {
	path, err := CredentialsPath()
	if err != nil {
		return "", false
	}
	m, err := readCredentials(path)
	if err != nil {
		return "", false
	}
	tok, ok := m[target]
	return tok, ok
}

// SaveToken writes target: token atomically at 0600.
func SaveToken(target, token string) error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}
	m, err := readCredentials(path)
	if err != nil {
		return err
	}
	m[target] = token
	return writeCredentials(path, m)
}

// DeleteToken removes the entry for target; missing entry is a no-op.
func DeleteToken(target string) error {
	path, err := CredentialsPath()
	if err != nil {
		return err
	}
	m, err := readCredentials(path)
	if err != nil {
		return err
	}
	delete(m, target)
	return writeCredentials(path, m)
}

// Logout forgets a stored token for target.
func Logout(target string) error { return DeleteToken(target) }

// Login prompts for a token on stdin, verifies it against target, and stores it.
func Login(ctx context.Context, target string) error {
	fmt.Fprint(os.Stderr, "Token: ")
	var tok string
	if term.IsTerminal(int(os.Stdin.Fd())) {
		b, err := term.ReadPassword(int(os.Stdin.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return err
		}
		tok = strings.TrimSpace(string(b))
	} else {
		line, err := bufio.NewReader(os.Stdin).ReadString('\n')
		if err != nil {
			return err
		}
		tok = strings.TrimSpace(line)
	}
	if tok == "" {
		return errors.New("empty token")
	}
	c, err := mikyu.Dial(target, mikyu.WithAuthToken(tok))
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.ListQueues(ctx); err != nil {
		return err // ErrUnauthorized reaches the caller; nothing is saved
	}
	return SaveToken(target, tok)
}
