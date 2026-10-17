package command

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/credentials"
	"golang.org/x/term"
)

// CredentialsPath returns the absolute path for the user's credential store.
func CredentialsPath() (string, error) { return credentials.Path() }

// LoadToken reads the stored token for target, or ("", false) when none is stored.
func LoadToken(target string) (string, bool) { return credentials.Load(target) }

// Logout forgets a stored token for target.
func Logout(target string) error { return credentials.Delete(target) }

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
	return credentials.Save(target, tok)
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
