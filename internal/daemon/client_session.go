package daemon

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"time"

	"github.com/dmunish/mikyu/internal/credentials"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// clientSession resolves the session a client-side daemon subcommand presents
// to an already-running daemon: built-in defaults, then MIKYU_* environment
// variables, then explicit flags, then the stored login credential as the
// token fallback. The config file plays no part — it configures a daemon, not
// a client. A non-nil base seeds the session with values a caller such as the
// REPL already resolved; the base carries its env, so environment variables
// are skipped in that case. The extra callbacks register a subcommand's
// presentation flags on the same flag set. A --help request prints usage to
// stdout and returns flag.ErrHelp; any other parse mistake prints usage to
// stderr wrapped in ErrUsage.
func clientSession(name string, args []string, stdout, stderr io.Writer, base *spec.Session, extra ...func(*flag.FlagSet)) (spec.Session, error) {
	sess := spec.BuiltinSession()
	if base != nil {
		sess = *base
	} else if err := spec.ApplyEnvSession(&sess, os.Getenv); err != nil {
		return spec.Session{}, err
	}

	var out bytes.Buffer
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(&out)
	bind := func(key string, set func(*spec.Session, spec.Value)) {
		p, ok := spec.Lookup(key)
		if !ok {
			panic("daemon: " + key + " is missing from spec.Table")
		}
		fv := &sessionFlag{param: p, set: func(v spec.Value) { set(&sess, v) }}
		if p.Kind == spec.KindBool {
			fs.Var(&boolSessionFlag{fv}, p.FlagName, p.Doc)
			return
		}
		fs.Var(fv, p.FlagName, p.Doc)
	}
	bind("target", func(s *spec.Session, v spec.Value) { s.Target = v.Text() })
	bind("tls_enabled", func(s *spec.Session, v spec.Value) { s.TLS.Enabled = v.Bool() })
	bind("tls_cert", func(s *spec.Session, v spec.Value) { s.TLS.Cert = v.Text() })
	bind("tls_key", func(s *spec.Session, v spec.Value) { s.TLS.Key = v.Text() })
	bind("tls_client_ca", func(s *spec.Session, v spec.Value) { s.TLS.ClientCA = v.Text() })
	bind("auth_method", func(s *spec.Session, v spec.Value) { s.Auth.Method = v.Text() })
	bind("auth_token_file", func(s *spec.Session, v spec.Value) { s.Auth.TokenFile = v.Text() })
	// Registered so --help lists it, but rejected on use: a token on argv leaks via ps.
	fs.String("auth-token", "", "REJECTED; use --auth-token-file or mikyu login")
	for _, fn := range extra {
		fn(fs)
	}
	fs.Usage = func() {
		fmt.Fprintf(&out, "Usage: %s [flags]\n\nFlags:\n", name)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			_, _ = stdout.Write(out.Bytes())
			return spec.Session{}, flag.ErrHelp
		}
		_, _ = stderr.Write(out.Bytes())
		return spec.Session{}, fmt.Errorf("%w: %v", ErrUsage, err)
	}
	var flagErr error
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "auth-token" {
			flagErr = errors.New("--auth-token is rejected: a token on argv leaks via ps; use --auth-token-file or mikyu login")
		}
	})
	if flagErr != nil {
		fmt.Fprintf(stderr, "%s: %v\n", name, flagErr)
		return spec.Session{}, fmt.Errorf("%w: %v", ErrUsage, flagErr)
	}
	if err := spec.ValidateSession(sess); err != nil {
		fmt.Fprintf(stderr, "%s: %v\n", name, err)
		return spec.Session{}, fmt.Errorf("%w: %v", ErrUsage, err)
	}

	// The stored login credential is the last explicit layer: a token saved by
	// `mikyu login` speaks for this target when no flag or env var did.
	if sess.Auth.Token == "" && sess.Auth.TokenFile == "" {
		if tok, ok := credentials.Load(sess.Target); ok {
			sess.Auth.Token = tok
		}
	}
	return sess, nil
}

// sessionFlag binds one spec.Table parameter to a flag.Value writing straight
// into a session; only flags the user actually passed ever Set, so env and
// built-in values survive untouched.
type sessionFlag struct {
	param spec.Param
	set   func(spec.Value)
}

func (f *sessionFlag) String() string { return "" }

func (f *sessionFlag) Set(text string) error {
	v, err := f.param.Parse(text)
	if err != nil {
		return err
	}
	f.set(v)
	return nil
}

// boolSessionFlag lets a boolean parameter stand alone as a bare flag.
type boolSessionFlag struct{ *sessionFlag }

func (f *boolSessionFlag) IsBoolFlag() bool { return true }

// replyError renders an OpError frame as a Go error, giving the unauthorized
// case its own actionable message.
func replyError(op string, body []byte) error {
	code, msg := protocol.DecodeError(body)
	if code == protocol.ErrCodeUnauthorized {
		return fmt.Errorf("%s: unauthorized; present a token with --auth-token-file or mikyu login", op)
	}
	return fmt.Errorf("%s failed: %s (code 0x%02x)", op, msg, code)
}

// dialTimeout bounds one client subcommand's connection attempt; probing or
// stopping a daemon should fail fast, not hang.
const dialTimeout = 2 * time.Second

// connect dials target, completes the one OpAuth handshake with the
// session's credential, and returns the connection ready for a follow-up op.
func connect(target string, sess spec.Session) (net.Conn, error) {
	network, addr, err := protocol.ParseTarget(target)
	if err != nil {
		return nil, err
	}
	dialer := &net.Dialer{Timeout: dialTimeout}
	var conn net.Conn
	if network == "tcp" && sess.TLS.Enabled {
		tlsCfg, err := clientTLSConfig(sess)
		if err != nil {
			return nil, err
		}
		conn, err = (&tls.Dialer{Config: tlsCfg, NetDialer: dialer}).Dial(network, addr)
	} else {
		conn, err = dialer.Dial(network, addr)
	}
	if err != nil {
		return nil, fmt.Errorf("no daemon serving %s (start one with: mikyu daemon run --data-dir <dir>): %w", target, err)
	}

	method, cred, err := resolveCredential(sess)
	if err != nil {
		conn.Close()
		return nil, err
	}
	var ab protocol.Buffer
	protocol.EncodeAuth(&ab, method, cred)
	if err := protocol.WriteFrame(conn, protocol.OpAuth, ab.Bytes()); err != nil {
		conn.Close()
		return nil, err
	}
	respOp, respBody, err := protocol.ReadFrame(bufio.NewReader(conn))
	if err != nil {
		conn.Close()
		return nil, err
	}
	if respOp == protocol.OpError {
		conn.Close()
		return nil, replyError("auth", respBody)
	}
	return conn, nil
}
