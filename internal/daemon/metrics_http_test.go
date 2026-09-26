package daemon

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testClient returns a client without keep-alive so no idle connection goroutine
// outlives the request for goleak to flag.
func testClient() *http.Client {
	return &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
}

// startMetricsDaemon runs the daemon with the collector ticking at 50ms and the
// metrics endpoint bound on listen.
func startMetricsDaemon(tb *testing.T, listen string, extra ...string) (finish func()) {
	tb.Helper()
	tmp := tb.TempDir()
	cfgPath := writeConfig(tb, tmp, "mikyu.yaml", "defaults:\n  flush:\n    max_bytes: 102400\n")
	target := "tcp://" + freeTCPAddr(tb)
	args := append([]string{
		"--config", cfgPath,
		"--target", target,
		"--data-dir", filepath.Join(tmp, "data"),
		"--metrics-enabled",
		"--metrics-interval", "50ms",
		"--metrics-listen", listen,
	}, extra...)
	return startDaemon(tb, target, args...)
}

func TestMetricsHTTPPrometheus(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	res, err := testClient().Get("http://" + listen + "/metrics")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	assert.True(t, strings.HasPrefix(res.Header.Get("Content-Type"), prometheusContentType), "got %q", res.Header.Get("Content-Type"))

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), "mikyu_runtime_uptime_seconds")
	assert.Contains(t, string(body), "mikyu_totals_queues")
}

// The preregistered server scope yields a queue view, so per-queue mode must
// label its series even before any traffic flows.
func TestMetricsHTTPPrometheusPerQueue(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen, "--metrics-per-queue-prometheus")
	defer finish()

	res, err := testClient().Get("http://" + listen + "/metrics")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)

	body, err := io.ReadAll(res.Body)
	require.NoError(t, err)
	assert.Contains(t, string(body), `queue="server"`)
}

func TestMetricsHTTPJSON(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	res, err := testClient().Get("http://" + listen + "/api/metrics")
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	assert.True(t, strings.HasPrefix(res.Header.Get("Content-Type"), "application/json"))

	var snap metrics.Snapshot
	require.NoError(t, json.NewDecoder(res.Body).Decode(&snap))
	assert.Positive(t, snap.Runtime.Goroutines)
	_, ok := snap.Queue("server")
	assert.True(t, ok)
}

func TestMetricsHTTPHistory(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	client := testClient()
	var hist []metrics.Snapshot
	require.Eventually(t, func() bool {
		res, err := client.Get("http://" + listen + "/api/history?limit=3")
		if err != nil || res.StatusCode != http.StatusOK {
			if err == nil {
				res.Body.Close()
			}
			return false
		}
		defer res.Body.Close()
		hist = nil
		if err := json.NewDecoder(res.Body).Decode(&hist); err != nil {
			return false
		}
		return len(hist) >= 2
	}, 3*time.Second, 25*time.Millisecond, "history should fill at a 50ms tick")
	assert.LessOrEqual(t, len(hist), 3)
}

func TestMetricsHTTPHistoryBadLimit(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	for _, q := range []string{"limit=0", "limit=abc", "limit=-2"} {
		res, err := testClient().Get("http://" + listen + "/api/history?" + q)
		require.NoError(t, err)
		res.Body.Close()
		assert.Equal(t, http.StatusBadRequest, res.StatusCode, "query %s", q)
	}
}

// readSSEEvent reads one server-sent event and returns its data payload.
func readSSEEvent(r *bufio.Reader) (string, error) {
	var data strings.Builder
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimSuffix(line, "\n")
		if line == "" {
			if data.Len() > 0 {
				return data.String(), nil
			}
			continue
		}
		if payload, ok := strings.CutPrefix(line, "data: "); ok {
			data.WriteString(payload)
		}
	}
}

func TestMetricsHTTPSSE(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	reqCtx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+listen+"/api/events", nil)
	require.NoError(t, err)
	res, err := testClient().Do(req)
	require.NoError(t, err)
	defer res.Body.Close()
	require.Equal(t, http.StatusOK, res.StatusCode)
	assert.Equal(t, "text/event-stream", res.Header.Get("Content-Type"))

	data, err := readSSEEvent(bufio.NewReader(res.Body))
	require.NoError(t, err, "a snapshot event must arrive within one 50ms tick")

	var snap metrics.Snapshot
	require.NoError(t, json.Unmarshal([]byte(data), &snap))
	assert.NotZero(t, snap.Timestamp)
}

func TestMetricsHTTPSSESlowClientDoesNotBlock(t *testing.T) {
	listen := freeTCPAddr(t)
	finish := startMetricsDaemon(t, listen)
	defer finish()

	reqCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, "http://"+listen+"/api/events", nil)
	require.NoError(t, err)
	slow, err := testClient().Do(req)
	require.NoError(t, err)
	t.Cleanup(func() {
		cancel()
		_ = slow.Body.Close()
	})

	// At least three ticks: the collector must be dropping frames for this
	// stalled subscriber, not blocking.
	time.Sleep(250 * time.Millisecond)

	res, err := testClient().Get("http://" + listen + "/api/metrics")
	require.NoError(t, err, "scraper must stay responsive with a stalled SSE client")
	res.Body.Close()
	assert.Equal(t, http.StatusOK, res.StatusCode)
}

func TestMetricsHTTPDisabledByDefault(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := writeConfig(t, tmp, "mikyu.yaml", "defaults:\n  flush:\n    max_bytes: 102400\n")
	target := "tcp://" + freeTCPAddr(t)

	startDaemon(t, target, "--config", cfgPath, "--target", target, "--data-dir", filepath.Join(tmp, "data"))()

	conn, err := net.DialTimeout("tcp", freeTCPAddr(t), 200*time.Millisecond)
	require.Error(t, err, "no listener may bind when metrics_listen is empty")
	if conn != nil {
		conn.Close()
	}
}

// A failed metrics bind must end Run with an error after the wire server is
// stopped, mirroring the wire-bind-failure contract.
func TestMetricsHTTPBindFailureReturnsError(t *testing.T) {
	tmp := t.TempDir()
	cfgPath := writeConfig(t, tmp, "mikyu.yaml", "defaults:\n  flush:\n    max_bytes: 102400\n")

	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer busy.Close()

	done := make(chan error, 1)
	go func() {
		done <- Run([]string{
			"--config", cfgPath,
			"--target", "tcp://" + freeTCPAddr(t),
			"--data-dir", filepath.Join(tmp, "data"),
			"--metrics-listen", busy.Addr().String(),
		})
	}()

	select {
	case err := <-done:
		require.Error(t, err)
		assert.Contains(t, err.Error(), "metrics http")
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after a metrics bind failure")
	}
}
