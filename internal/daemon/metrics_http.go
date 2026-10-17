package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
)

// topNQueues caps the per-queue series the Prometheus endpoint emits, largest queues first.
const topNQueues = 50

// metricsServer serves one node's telemetry over HTTP: Prometheus text, JSON snapshot, tick history, and the SSE stream.
type metricsServer struct {
	node *node.Node
	srv  *http.Server
}

// startMetricsHTTP binds addr and serves n's telemetry on four fixed routes; the listener is live before it returns.
func startMetricsHTTP(n *node.Node, perQueue, histograms bool, addr string) (*metricsServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	warnIfNotLoopback(ln.Addr())

	promReg := prometheus.NewRegistry()
	promReg.MustRegister(metrics.NewSnapshotCollector(
		func() (metrics.Snapshot, error) { return n.Metrics(context.Background()) },
		metrics.CollectOptions{PerQueue: perQueue, TopNQueues: topNQueues, Histograms: histograms},
	))

	ms := &metricsServer{node: n}
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(promReg, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	}))
	mux.HandleFunc("GET /api/metrics", ms.handleJSON)
	mux.HandleFunc("GET /api/history", ms.handleHistory)
	mux.HandleFunc("GET /api/events", ms.handleSSE)
	ms.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go ms.srv.Serve(ln)
	return ms, nil
}

// Stop shuts the server down, blocking until the serve loop and in-flight requests finish.
func (ms *metricsServer) Stop(ctx context.Context) error {
	return ms.srv.Shutdown(ctx)
}

// warnIfNotLoopback flags listeners reachable from beyond the host; the endpoint is documentation-secured, not firewall-secured.
func warnIfNotLoopback(addr net.Addr) {
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return
	}
	if host == "127.0.0.1" || strings.Trim(host, "[]") == "::1" {
		return
	}
	slog.Warn("metrics endpoint is reachable outside the host", "addr", addr)
}

// writeJSON encodes v as an application/json response body.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("metrics json encode failed", "err", err)
	}
}

// handleJSON serves a fresh snapshot as JSON, the same encoding as the wire and CLI surfaces.
func (ms *metricsServer) handleJSON(w http.ResponseWriter, r *http.Request) {
	snap, err := ms.node.Metrics(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	writeJSON(w, snap)
}

// handleHistory serves tick snapshots, newest last, keeping at most ?limit=N of them.
func (ms *metricsServer) handleHistory(w http.ResponseWriter, r *http.Request) {
	limit := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = n
	}
	hist := ms.node.Collector().History()
	if limit > 0 && len(hist) > limit {
		hist = hist[len(hist)-limit:]
	}
	if hist == nil {
		hist = []metrics.Snapshot{}
	}
	writeJSON(w, hist)
}

// handleSSE streams tick snapshots as server-sent events until the client disconnects.
func (ms *metricsServer) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	flusher.Flush()

	ch, cancel := ms.node.Collector().Subscribe()
	defer cancel()

	for {
		select {
		case <-r.Context().Done():
			return
		case snap, ok := <-ch:
			if !ok {
				return
			}
			data, err := json.Marshal(snap)
			if err != nil {
				return
			}
			fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}
