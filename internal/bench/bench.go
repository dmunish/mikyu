package bench

import (
	"context"
	"fmt"
	"io"
	"os"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
)

// Run executes one bench run against the embedded engine and returns its final result.
func Run(ctx context.Context, opts Options) (result.Result, error) {
	opts = normalizeOptions(opts)
	if err := opts.Workload.Validate(); err != nil {
		return result.Result{}, err
	}
	if err := spec.ValidateCreate(spec.Resolve(opts.Spec)); err != nil {
		return result.Result{}, err
	}
	if err := validateAccess(opts); err != nil {
		return result.Result{}, err
	}
	if err := validateMode(opts); err != nil {
		return result.Result{}, err
	}
	if err := validateMetrics(opts.Metrics); err != nil {
		return result.Result{}, err
	}

	runID := opts.RunID
	if runID == "" {
		runID = generateRunID()
	}
	names := queueNames(runID, opts.Workload.Queues)

	handle, err := openClient(ctx, opts)
	if err != nil {
		return result.Result{}, err
	}
	defer func() { _ = handle.Cleanup() }()

	if opts.Access != AccessRemote {
		release, err := acquire(ctx, handle.Client, opts.Dir, runID, queueRefs(names, opts.Workload.Kind), handle.DaemonPID, !opts.NoCleanupStale, nil)
		if err != nil {
			return result.Result{}, err
		}
		defer func() { _ = release() }()
	}

	if err := createQueues(ctx, handle.Client, names, opts); err != nil {
		return result.Result{}, err
	}

	if opts.Access != AccessRemote && !opts.Persist {
		defer func() {
			for _, name := range names {
				_ = handle.Client.DeleteQueue(ctx, name)
			}
		}()
	}

	r := &runner{opts: opts, client: handle.Client, queueNames: names, measure: NewMeasure()}
	if err := r.openQueues(ctx); err != nil {
		return result.Result{}, err
	}
	if opts.Verify {
		r.measure.MarkVerified()
	}

	env, err := newEnvelope(opts.Out, result.Result{
		Version:      result.CurrentVersion,
		RunID:        runID,
		Mode:         string(opts.Mode),
		Access:       string(opts.Access),
		AccessTarget: handle.Target,
		StartedAt:    time.Now(),
		Spec:         opts.Spec,
		Workload:     opts.Workload.View(),
		Environment:  buildEnvironment(opts),
	})
	if err != nil {
		return result.Result{}, err
	}

	var obs *observer
	if !opts.DisableObservation && opts.Mode != ModeRecovery {
		obs = newObserver(handle.Client, env, r.measure, r.phaseOf, names, opts.Workload.Kind, opts.Stderr)
		obs.Start(ctx)
	}

	stopProgress := r.startProgress(ctx)
	runErr := r.dispatch(ctx)
	stopProgress()
	if obs != nil {
		obs.Stop()
	}

	snap := r.measure.Snapshot()
	snap.BoundFired = r.boundFired()
	snap.Verified = opts.Verify
	snap.Recovery = r.recovery
	if secs := r.measureSpan().Seconds(); secs > 0 {
		snap.ThroughputMsgs = float64(snap.Messages) / secs
		snap.ThroughputBytes = snap.ThroughputMsgs * float64(opts.Workload.MsgSize)
	}

	aborted := ""
	if runErr != nil {
		aborted = runErr.Error()
	}
	if err := env.Finish(&snap, r.phasesCopy(), aborted); err != nil && runErr == nil {
		runErr = err
	}
	return env.result(), runErr
}

// normalizeOptions fills the defaults the command layer leaves for the runner.
func normalizeOptions(opts Options) Options {
	if opts.Mode == "" {
		opts.Mode = DefaultMode
	}
	if opts.Access == "" {
		opts.Access = AccessEmbedded
	}
	if opts.Out == "" {
		opts.Out = "bench"
	}
	if opts.Stderr == nil {
		opts.Stderr = io.Discard
	}
	if len(opts.Metrics) == 0 {
		opts.Metrics = slices.Clone(defaultMetrics)
	}
	opts.Spec = opts.Spec.Merge(DefaultSpec())
	if opts.CPU > 0 {
		runtime.GOMAXPROCS(opts.CPU)
	}
	return opts
}

// defaultMetrics is the metric list a run reports when the caller names none.
var defaultMetrics = []string{"throughput"}

// knownMetrics names every series column a run may report.
var knownMetrics = []string{
	"throughput", "p50", "p90", "p99", "queue_depth", "size_bytes", "rss_bytes", "cpu_percent",
}

// validateMetrics rejects a metric the series does not carry.
func validateMetrics(metrics []string) error {
	for _, m := range metrics {
		if !slices.Contains(knownMetrics, m) {
			return fmt.Errorf("%w: unknown metric %q", ErrUsage, m)
		}
	}
	return nil
}

// validateAccess enforces the flag combination each access mode needs.
func validateAccess(opts Options) error {
	switch opts.Access {
	case AccessEmbedded, AccessDaemon:
		if opts.Dir == "" {
			return fmt.Errorf("%w: %s access requires --dir", ErrUsage, opts.Access)
		}
		if opts.Target != "" {
			return fmt.Errorf("%w: --dir and --target are mutually exclusive", ErrUsage)
		}
	case AccessRemote:
		if opts.Target == "" {
			return fmt.Errorf("%w: remote access requires --target", ErrUsage)
		}
	default:
		return fmt.Errorf("%w: unknown access %q", ErrUsage, opts.Access)
	}
	return nil
}

// validateMode enforces the constraints a single mode adds on top of access.
func validateMode(opts Options) error {
	if opts.Mode != ModeRecovery {
		return nil
	}
	if opts.Workload.Queues > 1 {
		return errRecoveryQueues
	}
	if opts.Access != AccessEmbedded {
		return fmt.Errorf("%w: recovery mode requires embedded access", ErrUsage)
	}
	return nil
}

// runner holds everything one mode needs to drive a workload.
type runner struct {
	opts       Options
	client     mikyu.Client
	measure    *Measure
	recovery   *result.RecoveryMetrics
	logQueues  []mikyu.LogQueue
	taskQueues []mikyu.TaskQueue
	queueNames []string

	seq        atomic.Uint64
	written    atomic.Int64
	consumed   atomic.Int64
	cursorPick atomic.Uint64
	cursors    []*queueCursor

	measureMu    sync.Mutex
	measureStart time.Time
	measureBegin time.Time
	measureEnd   time.Time

	phasesMu sync.Mutex
	phases   []result.PhaseView
	current  Phase
	bound    boundary
}

// dispatch routes the run to its mode.
func (r *runner) dispatch(ctx context.Context) error {
	switch r.opts.Mode {
	case ModeProduce:
		return runProduce(ctx, r)
	case ModeConsume:
		return runConsume(ctx, r)
	case ModeMix:
		return runMix(ctx, r)
	case ModeRecovery:
		return runRecovery(ctx, r)
	default:
		return fmt.Errorf("%w: unknown mode %q", ErrUsage, r.opts.Mode)
	}
}

// openQueues resolves a driver handle for every queue the run created.
func (r *runner) openQueues(ctx context.Context) error {
	if r.isLog() {
		for _, name := range r.queueNames {
			q, err := r.client.Log(ctx, name)
			if err != nil {
				return err
			}
			r.logQueues = append(r.logQueues, q)
		}
		return nil
	}
	for _, name := range r.queueNames {
		q, err := r.client.Task(ctx, name)
		if err != nil {
			return err
		}
		r.taskQueues = append(r.taskQueues, q)
	}
	return nil
}

// isLog reports whether the run targets log queues.
func (r *runner) isLog() bool { return r.opts.Workload.Kind == KindLog }

// openLoop reports whether the workload is rate-throttled.
func (r *runner) openLoop() bool {
	w := r.opts.Workload
	return w.Rate > 0 || len(w.RateSchedule) > 0
}

// limitReached reports whether a byte budget or the measure-phase duration bound has fired.
func (r *runner) limitReached(moved int64) bool {
	w := r.opts.Workload
	if w.Bytes > 0 && moved >= w.Bytes {
		return true
	}
	return w.Duration > 0 && r.elapsedMeasure() >= w.Duration
}

// boundFired names the bound that stopped the run, or "drained" when the source ran out first.
func (r *runner) boundFired() string {
	w := r.opts.Workload
	switch {
	case w.CrashAfterBytes > 0:
		return "crash"
	case w.Bytes > 0 && r.moved() >= w.Bytes:
		return "bytes"
	case w.Duration > 0 && r.elapsedMeasure() >= w.Duration:
		return "duration"
	case w.Bytes > 0 || w.Duration > 0:
		return "drained"
	default:
		return ""
	}
}

// beginPhase opens a phase and reports it.
func (r *runner) beginPhase(p Phase) {
	r.phasesMu.Lock()
	r.current = p
	r.bound.begin(p, time.Now())
	r.phasesMu.Unlock()
	emitProgress(r.opts, ProgressEvent{
		Kind:  ProgressStart,
		Phase: p,
		Total: r.byteBound(),
	})
}

// endPhase closes the current phase into a view.
func (r *runner) endPhase(now time.Time) {
	r.phasesMu.Lock()
	p := r.current
	view := r.bound.close(now)
	r.phases = append(r.phases, view)
	r.phasesMu.Unlock()
	emitProgress(r.opts, ProgressEvent{Kind: ProgressEnd, Phase: p, Bytes: view.Bytes})
}

// addPhaseBytes accumulates bytes moved in the current phase.
func (r *runner) addPhaseBytes(n int64) {
	r.phasesMu.Lock()
	r.bound.add(n)
	r.phasesMu.Unlock()
}

func (r *runner) phaseBytes() int64 {
	r.phasesMu.Lock()
	defer r.phasesMu.Unlock()
	return r.bound.bytes
}

// moved returns the bytes this run has moved so far on its measured path.
func (r *runner) moved() int64 {
	if r.opts.Mode == ModeConsume {
		return r.consumed.Load()
	}
	return r.written.Load()
}

// byteBound returns the run's byte budget, or 0 when the run is duration bound.
func (r *runner) byteBound() int64 {
	switch w := r.opts.Workload; {
	case w.CrashAfterBytes > 0:
		return w.CrashAfterBytes
	case w.Bytes > 0:
		return w.Bytes
	default:
		return 0
	}
}

// phaseOf reports the phase in progress for the observer.
func (r *runner) phaseOf() Phase {
	r.phasesMu.Lock()
	defer r.phasesMu.Unlock()
	return r.current
}

// phasesCopy returns the completed phase views.
func (r *runner) phasesCopy() []result.PhaseView {
	r.phasesMu.Lock()
	defer r.phasesMu.Unlock()
	return slices.Clone(r.phases)
}

// beginMeasure starts the measured window.
func (r *runner) beginMeasure() {
	now := time.Now()
	r.measureMu.Lock()
	r.measureStart = now
	r.measureBegin = now
	r.measureMu.Unlock()
}

// endMeasure closes the measured window.
func (r *runner) endMeasure(now time.Time) {
	r.measureMu.Lock()
	r.measureEnd = now
	r.measureMu.Unlock()
}

// measureSpan returns how long the measured window was.
func (r *runner) measureSpan() time.Duration {
	r.measureMu.Lock()
	defer r.measureMu.Unlock()
	if r.measureBegin.IsZero() || r.measureEnd.IsZero() {
		return 0
	}
	return r.measureEnd.Sub(r.measureBegin)
}

// elapsedMeasure returns how long the measured window has run so far.
func (r *runner) elapsedMeasure() time.Duration {
	r.measureMu.Lock()
	defer r.measureMu.Unlock()
	if r.measureStart.IsZero() {
		return 0
	}
	return time.Since(r.measureStart)
}

// queueRefs names each queue with the run's structural kind.
func queueRefs(names []string, kind string) []queueRef {
	refs := make([]queueRef, len(names))
	for i, name := range names {
		refs[i] = queueRef{Name: name, Kind: kind}
	}
	return refs
}

// createQueues declares every queue the run owns before the envelope opens.
func createQueues(ctx context.Context, client mikyu.Client, names []string, opts Options) error {
	opt := specOption(opts.Spec)
	for _, name := range names {
		var err error
		if opts.Workload.Kind == KindTask {
			_, err = client.CreateTask(ctx, name, opt)
		} else {
			_, err = client.CreateLog(ctx, name, opt)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// specOption applies a whole spec at the SDK boundary.
func specOption(s spec.Spec) mikyu.QueueOption {
	return func(dst *mikyu.Spec) { *dst = s }
}

// Cache states a run may record for the OS page cache.
const (
	cacheWarm    = "warm"
	cacheCold    = "cold"
	cacheUnknown = "unknown"
)

// buildEnvironment records the build and host the numbers came from, dropping the page cache when asked.
func buildEnvironment(opts Options) result.EnvironmentView {
	host, _ := os.Hostname()
	env := result.EnvironmentView{
		Host:            host,
		OS:              runtime.GOOS,
		Arch:            runtime.GOARCH,
		GoVersion:       runtime.Version(),
		SpecVersion:     spec.CurrentSpecVersion,
		BenchVersion:    result.CurrentVersion,
		ClientCPU:       runtime.GOMAXPROCS(0),
		CacheState:      cacheWarm,
		TimerOverheadNS: int64(TimerOverhead()),
	}
	if opts.Cold {
		verified, _ := dropCache(opts.Dir)
		env.ColdVerified = verified
		env.CacheState = cacheUnknown
		if verified {
			env.CacheState = cacheCold
		}
	}
	return env
}
