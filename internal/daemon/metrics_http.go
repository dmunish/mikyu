package daemon

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/node"
)

// topNQueues caps the per-queue series the Prometheus endpoint emits, largest queues first.
const topNQueues = 50

// prometheusContentType is the media type the Prometheus text format requires.
const prometheusContentType = "text/plain; version=0.0.4; charset=utf-8"

// metricsServer serves one node's telemetry over HTTP: Prometheus text, JSON snapshot, tick history, and the SSE stream.
type metricsServer struct {
	node       *node.Node
	perQueue   bool
	histograms bool
	srv        *http.Server
}

// startMetricsHTTP binds addr and serves n's telemetry on four fixed routes; the listener is live before it returns.
func startMetricsHTTP(n *node.Node, perQueue, histograms bool, addr string) (*metricsServer, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	warnIfNotLoopback(ln.Addr())

	ms := &metricsServer{node: n, perQueue: perQueue, histograms: histograms}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", ms.handlePrometheus)
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
	log.Printf("mikyu daemon: warning: metrics server at %s is reachable outside the host", addr)
}

// writeJSON encodes v as an application/json response body.
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("mikyu daemon: metrics json encode: %v", err)
	}
}

// handlePrometheus encodes a fresh snapshot in the Prometheus text format 0.0.4.
func (ms *metricsServer) handlePrometheus(w http.ResponseWriter, r *http.Request) {
	snap, err := ms.node.Metrics(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", prometheusContentType)
	if err := metrics.Encode(w, snap, metrics.EncodeOptions{PerQueue: ms.perQueue, TopNQueues: topNQueues, Histograms: ms.histograms}); err != nil {
		log.Printf("mikyu daemon: prometheus encode: %v", err)
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
