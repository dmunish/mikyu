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
		"tls_enabled":                  ScopeBoth,
		"tls_cert_file":                ScopeBoth,
		"tls_key_file":                 ScopeBoth,
		"max_conns":                    ScopeBoth,
		"metrics_enabled":              ScopeBoth,
		"metrics_interval":             ScopeBoth,
		"metrics_history":              ScopeBoth,
		"metrics_listen":               ScopeServer,
		"metrics_per_queue_prometheus": ScopeServer,
		"metrics_histograms":           ScopeBoth,
		"auth_method":                  ScopeServer,
		"auth_token_file":              ScopeBoth,
		"auth_token":                   ScopeBoth,
		"tls_client_ca_file":           ScopeServer,
	}

	require.Len(t, want, len(Params(LayerSession)), "a session key was added or removed")
	for _, p := range Params(LayerSession) {
		assert.Equal(t, want[p.Key], p.Scope, p.Key)
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
	assert.Equal(t, []string{"metrics_listen", "metrics_per_queue_prometheus", "auth_method", "tls_client_ca_file"}, keys(server))

	both := keys(ParamsInScope(LayerSession, ScopeBoth))
	assert.Contains(t, both, "max_conns", "max_conns configures both the client pool and the server cap")
	assert.NotContains(t, both, "metrics_listen", "a server-only parameter leaked into both")

	visible := ParamsInScope(LayerSession, ScopeClient, ScopeBoth)
	assert.Equal(t, len(Params(LayerSession))-len(server), len(visible), "client and both scopes are the client-visible flags")
}
