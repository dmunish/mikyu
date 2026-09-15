package spec

import (
	"slices"
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
		"max_conns":                    ScopeClient,
		"metrics_enabled":              ScopeBoth,
		"metrics_interval":             ScopeBoth,
		"metrics_history":              ScopeBoth,
		"metrics_listen":               ScopeServer,
		"metrics_per_queue_prometheus": ScopeServer,
		"metrics_histograms":           ScopeBoth,
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
	assert.Equal(t, []string{"max_conns"}, keys(client))

	server := ParamsInScope(LayerSession, ScopeServer)
	assert.Equal(t, []string{"metrics_listen", "metrics_per_queue_prometheus"}, keys(server))

	both := ParamsInScope(LayerSession, ScopeClient, ScopeBoth)
	assert.Equal(t, len(Params(LayerSession))-len(server), len(both), "client and both scopes are the client-visible flags")
	assert.False(t, slices.Contains(keys(both), "metrics_listen"), "a server-scope parameter leaked into the client flags")
}
