package server

import (
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"os"
	"strings"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

const minTokenBytes = 16

// authMode resolves a session's auth settings into a per-connection verifier.
type authMode struct {
	method byte
	token  []byte
}

// NewAuthMode resolves a session's auth settings into a verifier.
func NewAuthMode(sess spec.Session) (*authMode, error) {
	switch sess.AuthMethod {
	case "", "none", "none-insecure":
		return &authMode{method: protocol.AuthMethodNone}, nil
	case "token":
		tok := sess.AuthToken
		if tok == "" && sess.AuthTokenFile != "" {
			data, err := os.ReadFile(sess.AuthTokenFile)
			if err != nil {
				return nil, err
			}
			tok = strings.TrimSpace(string(data))
		}
		if len(tok) < minTokenBytes {
			return nil, errors.New("token credential is shorter than 16 bytes")
		}
		return &authMode{method: protocol.AuthMethodToken, token: []byte(tok)}, nil
	case "mtls":
		return &authMode{method: protocol.AuthMethodMTLS}, nil
	}
	return nil, errors.New("unknown auth_method " + sess.AuthMethod)
}

// Required reports whether any non-anonymous credential is needed.
func (a *authMode) Required() bool { return a != nil && a.method != protocol.AuthMethodNone }

// Verify checks a client's claimed method and credential against the server's mode.
func (a *authMode) Verify(method byte, cred []byte, tlsState *tls.ConnectionState) error {
	switch a.method {
	case protocol.AuthMethodNone:
		return nil // anonymous server accepts anyone
	case protocol.AuthMethodToken:
		if method != protocol.AuthMethodToken || subtle.ConstantTimeCompare(cred, a.token) != 1 {
			return protocol.ErrUnauthorized
		}
		return nil
	case protocol.AuthMethodMTLS:
		if method != protocol.AuthMethodMTLS || tlsState == nil || len(tlsState.VerifiedChains) == 0 {
			return protocol.ErrUnauthorized
		}
		return nil
	}
	return protocol.ErrUnauthorized
}
