package command

import (
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

	opts = append(opts,
		mikyu.WithAuthMethod(sess.Auth.Method),
		mikyu.WithAuthToken(sess.Auth.Token),
		mikyu.WithAuthTokenFile(sess.Auth.TokenFile),
		mikyu.WithTLSClientCA(sess.TLS.ClientCA),
	)

	if err := daemon.EnsureDaemonRunning(daemon.SpawnOptions{
		Target:        sess.Target,
		DataDir:       sess.DataDir,
		TLSEnabled:    sess.TLS.Enabled,
		TLSCert:       sess.TLS.Cert,
		TLSKey:        sess.TLS.Key,
		AuthMethod:    sess.Auth.Method,
		AuthTokenFile: sess.Auth.TokenFile,
		TLSClientCA:   sess.TLS.ClientCA,
	}); err != nil {
		return nil, err
	}
	return mikyu.Dial(sess.Target, opts...)
}
