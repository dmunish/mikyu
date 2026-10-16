package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

func WithSessionSpec(s Spec) ClientOption { return client.WithSessionSpec(s) }

func WithTarget(target string) ClientOption { return client.WithTarget(target) }

func WithDataDir(dir string) ClientOption { return client.WithDataDir(dir) }

func WithMaxConnections(n int) ClientOption { return client.WithMaxConnections(n) }

func WithTLSEnabled(enabled bool) ClientOption { return client.WithTLSEnabled(enabled) }

func WithTLSCert(path string) ClientOption { return client.WithTLSCert(path) }

func WithTLSKey(path string) ClientOption { return client.WithTLSKey(path) }

func WithTLSClientCA(path string) ClientOption { return client.WithTLSClientCA(path) }

func WithAutoStart(enabled bool) ClientOption { return client.WithAutoStart(enabled) }

func WithAuthMethod(method string) ClientOption { return client.WithAuthMethod(method) }

func WithAuthToken(tok string) ClientOption { return client.WithAuthToken(tok) }

func WithAuthTokenFile(path string) ClientOption { return client.WithAuthTokenFile(path) }

func WithEncryptionKeyFile(path string) ClientOption { return client.WithEncryptionKeyFile(path) }

func WithMetricsEnabled(enabled bool) ClientOption { return client.WithMetricsEnabled(enabled) }

func WithMetricsInterval(d time.Duration) ClientOption { return client.WithMetricsInterval(d) }

func WithMetricsHistory(n int) ClientOption { return client.WithMetricsHistory(n) }

func WithMetricsListen(addr string) ClientOption { return client.WithMetricsListen(addr) }

func WithMetricsPerQueuePrometheus(enabled bool) ClientOption {
	return client.WithMetricsPerQueuePrometheus(enabled)
}

func WithMetricsHistograms(enabled bool) ClientOption { return client.WithMetricsHistograms(enabled) }
