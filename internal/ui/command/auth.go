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

// LoadToken reads the stored token for target, or ("", false) when none is stored.
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

// DeleteToken removes the entry for target; a missing entry is a no-op.
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

// Login prompts for a token on the CLI's stdin, verifies it, and stores it.
func Login(ctx context.Context, target string) error {
	tok, err := readTokenFrom(os.Stdin)
	if err != nil {
		return err
	}
	return LoginWithToken(ctx, target, tok)
}

// LoginWithToken verifies an already-obtained token against target before persisting it.
func LoginWithToken(ctx context.Context, target, tok string) error {
	tok = strings.TrimSpace(tok)
	if tok == "" {
		return errors.New("empty token")
	}
	c, err := mikyu.Dial(target, mikyu.WithAuthToken(tok))
	if err != nil {
		return err
	}
	defer c.Close()
	if _, err := c.ListQueues(ctx); err != nil {
		return err
	}
	return SaveToken(target, tok)
}

// readTokenFrom prompts on stderr and reads one token from f.
func readTokenFrom(f *os.File) (string, error) {
	fmt.Fprint(os.Stderr, "Token: ")
	if term.IsTerminal(int(f.Fd())) {
		b, err := term.ReadPassword(int(f.Fd()))
		fmt.Fprintln(os.Stderr)
		if err != nil {
			return "", err
		}
		return strings.TrimSpace(string(b)), nil
	}
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(line), nil
}
