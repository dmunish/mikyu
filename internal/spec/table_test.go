package spec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSessionScopesAreAssigned pins that every session parameter declares which side it configures.
func TestSessionScopesAreAssigned(t *testing.T) {
	for _, p := range Params(LayerSession) {
		require.NotEqual(t, ScopeUnspecified, p.Scope, p.Key)
	}
}

// TestSessionScopeAssignments pins the exact scope of every session key.
func TestSessionScopeAssignments(t *testing.T) {
	want := map[string]Scope{
		"target":                       ScopeBoth,
		"data_dir":                     ScopeBoth,
		"max_connections":              ScopeBoth,
		"tls_enabled":                  ScopeBoth,
		"tls_cert":                     ScopeBoth,
		"tls_key":                      ScopeBoth,
		"tls_client_ca":                ScopeServer,
		"metrics_enabled":              ScopeBoth,
		"metrics_interval":             ScopeBoth,
		"metrics_history":              ScopeBoth,
		"metrics_listen":               ScopeServer,
		"metrics_per_queue_prometheus": ScopeServer,
		"metrics_histograms":           ScopeBoth,
		"auth_method":                  ScopeServer,
		"auth_token_file":              ScopeBoth,
		"auth_token":                   ScopeBoth,
		"encryption_key_file":          ScopeServer,
	}

	require.Len(t, want, len(Params(LayerSession)), "a session key was added or removed")
	for _, p := range Params(LayerSession) {
		assert.Equal(t, want[p.Key], p.Scope, p.Key)
	}
}

// TestSessionPathsAreNested pins the nested server.* path of every session key.
func TestSessionPathsAreNested(t *testing.T) {
	want := map[string]string{
		"target":                       "server.target",
		"data_dir":                     "server.data_dir",
		"max_connections":              "server.max_connections",
		"tls_enabled":                  "server.tls.enabled",
		"tls_cert":                     "server.tls.cert",
		"tls_key":                      "server.tls.key",
		"tls_client_ca":                "server.tls.client_ca",
		"auth_method":                  "server.auth.method",
		"auth_token":                   "server.auth.token",
		"auth_token_file":              "server.auth.token_file",
		"encryption_key_file":          "server.encryption.key_file",
		"metrics_enabled":              "server.metrics.enabled",
		"metrics_interval":             "server.metrics.interval",
		"metrics_history":              "server.metrics.history",
		"metrics_listen":               "server.metrics.listen",
		"metrics_per_queue_prometheus": "server.metrics.per_queue_prometheus",
		"metrics_histograms":           "server.metrics.histograms",
	}

	require.Len(t, want, len(Params(LayerSession)), "a session key was added or removed")
	for _, p := range Params(LayerSession) {
		assert.Equal(t, want[p.Key], p.Path, p.Key)
	}
}

// TestParamsInScope checks the scope filter partitions the session layer.
func TestParamsInScope(t *testing.T) {
	keys := func(params []Param) []string {
		out := make([]string, 0, len(params))
		for _, p := range params {
			out = append(out, p.Key)
		}
		return out
	}

	client := ParamsInScope(LayerSession, ScopeClient)
	assert.Empty(t, keys(client), "no session parameter is client-only")

	server := ParamsInScope(LayerSession, ScopeServer)
	assert.Equal(t, []string{"tls_client_ca", "auth_method", "encryption_key_file", "metrics_listen", "metrics_per_queue_prometheus"}, keys(server))

	both := keys(ParamsInScope(LayerSession, ScopeBoth))
	assert.Contains(t, both, "max_connections", "max_connections configures both the client pool and the server cap")
	assert.NotContains(t, both, "metrics_listen", "a server-only parameter leaked into both")

	visible := ParamsInScope(LayerSession, ScopeClient, ScopeBoth)
	assert.Equal(t, len(Params(LayerSession))-len(server), len(visible), "client and both scopes are the client-visible flags")
}
