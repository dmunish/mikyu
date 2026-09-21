package command

import (
	"os"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/spec"
)

// SpecOption returns the single option carrying an already-built spec,
// which is how the front-ends hand table-built flag values to the SDK.
func SpecOption(s spec.Spec) mikyu.QueueOption {
	return func(dst *spec.Spec) { *dst = s }
}

// OpenClient returns an embedded client over the session's data
// directory, or a remote client for the session's target when no
// directory is set.
func OpenClient(sess spec.Session) (mikyu.Client, error) {
	opts := []mikyu.ClientOption{
		mikyu.WithMaxConns(sess.MaxConns),
		mikyu.WithTLSEnabled(sess.TLSEnabled),
		mikyu.WithTLSCertFile(sess.TLSCertFile),
		mikyu.WithTLSKeyFile(sess.TLSKeyFile),
		mikyu.WithMetricsEnabled(sess.MetricsEnabled),
		mikyu.WithMetricsInterval(sess.MetricsInterval.Duration()),
		mikyu.WithMetricsHistory(sess.MetricsHistory),
		mikyu.WithMetricsHistograms(sess.MetricsHistograms),
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
	if sess.AuthToken == "" && sess.AuthTokenFile == "" {
		if tok, ok := LoadToken(sess.Target); ok {
			sess.AuthToken = tok
		}
	}

	opts = append(opts,
		mikyu.WithAuthMethod(sess.AuthMethod),
		mikyu.WithAuthToken(sess.AuthToken),
		mikyu.WithAuthTokenFile(sess.AuthTokenFile),
		mikyu.WithTLSClientCaFile(sess.TLSClientCaFile),
	)

	if err := daemon.EnsureDaemonRunning(daemon.SpawnOptions{
		Target:          sess.Target,
		DataDir:         sess.DataDir,
		TLSEnabled:      sess.TLSEnabled,
		TLSCertFile:     sess.TLSCertFile,
		TLSKeyFile:      sess.TLSKeyFile,
		AuthMethod:      sess.AuthMethod,
		AuthTokenFile:   sess.AuthTokenFile,
		TLSClientCaFile: sess.TLSClientCaFile,
	}); err != nil {
		return nil, err
	}
	return mikyu.Dial(sess.Target, opts...)
}
