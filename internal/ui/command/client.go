package command

import (
	"fmt"
	"os"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
)

// SpecOption returns the single option carrying an already-built spec to the SDK.
func SpecOption(s spec.Spec) mikyu.QueueOption {
	return func(dst *spec.Spec) { *dst = s }
}

// OpenClient returns an embedded client for the session's data dir, or a remote client for its target.
func OpenClient(sess spec.Session) (mikyu.Client, error) {
	opts := []mikyu.ClientOption{
		mikyu.WithMaxConnections(sess.MaxConnections),
		mikyu.WithTLSEnabled(sess.TLS.Enabled),
		mikyu.WithTLSCert(sess.TLS.Cert),
		mikyu.WithTLSKey(sess.TLS.Key),
		mikyu.WithMetricsEnabled(sess.Metrics.Enabled),
		mikyu.WithMetricsInterval(sess.Metrics.Interval.Duration()),
		mikyu.WithMetricsHistory(sess.Metrics.History),
		mikyu.WithMetricsHistograms(sess.Metrics.Histograms),
	}

	if sess.DataDir != "" {
		var s mikyu.Spec
		if err := spec.ApplyEnv(&s, os.Getenv); err != nil {
			return nil, err
		}
		opts = append(opts, mikyu.WithEncryptionKeyFile(sess.Encryption.KeyFile))
		return mikyu.Open(sess.DataDir, append(opts, mikyu.WithSessionSpec(s))...)
	}

	if err := spec.ApplyEnvSession(&sess, os.Getenv); err != nil {
		return nil, err
	}
	if sess.Auth.Token == "" && sess.Auth.TokenFile == "" {
		if tok, ok := LoadToken(sess.Target); ok {
			sess.Auth.Token = tok
		}
	}

	// Fail fast with a hint instead of a raw dial error on the first
	// operation: a daemon is started explicitly with `mikyu daemon run`.
	if !daemon.IsDaemonAlive(sess.Target) {
		return nil, fmt.Errorf("no daemon serving %s (start one with: mikyu daemon run --data-dir <dir>)", sess.Target)
	}

	opts = append(opts,
		mikyu.WithAuthMethod(sess.Auth.Method),
		mikyu.WithAuthToken(sess.Auth.Token),
		mikyu.WithAuthTokenFile(sess.Auth.TokenFile),
		mikyu.WithTLSClientCA(sess.TLS.ClientCA),
	)
	return mikyu.Dial(sess.Target, opts...)
}
