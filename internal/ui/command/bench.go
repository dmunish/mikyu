package command

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"charm.land/bubbles/v2/progress"
	"github.com/goccy/go-yaml"

	"github.com/dmunish/mikyu/internal/bench"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/benchview"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// BenchArgs carries the destinations BenchFlagSet binds for one run.
type BenchArgs struct {
	Options bench.Options
	File    string

	cleanupStale bool
}

// BenchFlagSet builds the flag set for one run mode, binding into a.
func BenchFlagSet(mode bench.Mode, a *BenchArgs) *flag.FlagSet {
	fs := NewFlagSet("bench " + string(mode))
	a.cleanupStale = true
	for _, p := range bench.Table {
		if !p.AppliesTo(mode) {
			continue
		}
		fs.Var(&workloadValue{param: p, workload: &a.Options.Workload}, p.FlagName, p.Doc)
	}
	queueFlagSetInto(fs, &a.Options.Spec, spec.Params(spec.LayerSpec))
	if a.Options.Out == "" {
		a.Options.Out = "bench"
	}
	fs.StringVar(&a.Options.Dir, "dir", a.Options.Dir, "data directory (embedded and daemon access)")
	fs.StringVar(&a.Options.Target, "target", a.Options.Target, "remote target to dial")
	fs.Var(&accessValue{access: &a.Options.Access}, "access", "how to reach mikyu: embedded, daemon, or remote")
	fs.StringVar(&a.Options.Out, "out", a.Options.Out, "directory the run is written under")
	fs.BoolVar(&a.Options.Persist, "persist", false, "keep the spawned daemon and queues after the run")
	fs.BoolVar(&a.cleanupStale, "cleanup-stale", true, "remove queues recorded by previous runs")
	fs.BoolVar(&a.Options.Quiet, "quiet", false, "suppress the progress bar")
	fs.BoolVar(&a.Options.Verify, "verify", false, "verify every payload the run reads")
	fs.BoolVar(&a.Options.Cold, "cold", false, "drop the page cache before measuring")
	fs.IntVar(&a.Options.CPU, "cpu", 0, "GOMAXPROCS override; 0 leaves it alone")
	fs.Func("metric", "series metric to retain; repeatable", func(text string) error {
		a.Options.Metrics = append(a.Options.Metrics, splitCSV(text)...)
		return nil
	})
	fs.StringVar(&a.File, "file", "", "scenario YAML file (mode, spec, workload)")
	return fs
}

// PlotFlagSet binds the --metric flag for bench plot.
func PlotFlagSet(metrics *[]string) *flag.FlagSet {
	fs := NewFlagSet("bench plot")
	fs.Func("metric", "metric to chart; repeatable", func(text string) error {
		*metrics = append(*metrics, splitCSV(text)...)
		return nil
	})
	return fs
}

// RunBench resolves args, runs the benchmark, renders the summary, and returns the run directory.
func RunBench(ctx context.Context, mode bench.Mode, args []string, out, errw io.Writer, sess spec.Session) (string, error) {
	opts, err := resolveBench(mode, args, sess, os.Getenv)
	if err != nil {
		return "", err
	}
	opts.Stderr = errw
	opts.RunID = bench.NewRunID()
	opts.Out = filepath.Join(opts.Out, opts.RunID)

	printer := startProgressPrinter(errw, opts)
	if printer != nil {
		opts.Progress = printer.event
	}
	res, runErr := bench.Run(ctx, opts)
	if printer != nil {
		printer.stop()
	}
	if runErr != nil {
		return opts.Out, runErr
	}
	if err := Present(out, res, opts.Out); err != nil {
		return opts.Out, err
	}
	return opts.Out, nil
}

// Present renders a completed run's summary.
func Present(w io.Writer, r result.Result, dir string) error {
	theme, fm := benchPresentation(w)
	return benchview.Summary(w, r, dir, theme, fm)
}

// ShowRun renders a saved run's summary.
func ShowRun(w io.Writer, dir string) error {
	theme, fm := benchPresentation(w)
	return benchview.Show(w, dir, theme, fm)
}

// CompareRuns renders a diff over saved runs.
func CompareRuns(w io.Writer, dirs []string) error {
	theme, fm := benchPresentation(w)
	return benchview.Compare(w, dirs, theme, fm)
}

// PlotRun renders the requested series charts for a saved run.
func PlotRun(w io.Writer, dir string, metrics []string) error {
	theme, _ := benchPresentation(w)
	return benchview.Plot(w, dir, metrics, theme)
}

// resolveBench computes the run options from flags, a scenario file, and the environment.
func resolveBench(mode bench.Mode, args []string, sess spec.Session, lookup func(string) string) (bench.Options, error) {
	probe := BenchArgs{Options: bench.Options{Mode: mode, Dir: sess.DataDir}}
	rest, err := ParseAnywhere(BenchFlagSet(mode, &probe), args)
	if err != nil {
		return bench.Options{}, fmt.Errorf("%w: %v", bench.ErrUsage, err)
	}
	if len(rest) > 0 {
		return bench.Options{}, fmt.Errorf("%w: bench %s takes no positional arguments", bench.ErrUsage, mode)
	}

	opts := bench.Options{
		Mode:     mode,
		Access:   bench.AccessEmbedded,
		Dir:      sess.DataDir,
		Out:      "bench",
		Workload: bench.DefaultWorkload(),
	}
	applyBenchEnv(&opts, lookup)
	if probe.File != "" {
		if err := loadScenario(probe.File, mode, &opts); err != nil {
			return bench.Options{}, err
		}
	}

	final := BenchArgs{Options: opts}
	if _, err := ParseAnywhere(BenchFlagSet(mode, &final), args); err != nil {
		return bench.Options{}, fmt.Errorf("%w: %v", bench.ErrUsage, err)
	}
	final.Options.NoCleanupStale = !final.cleanupStale
	if final.Options.Access == "" {
		final.Options.Access = bench.AccessEmbedded
	}
	return final.Options, nil
}

// applyBenchEnv sets every workload parameter the environment provides.
func applyBenchEnv(opts *bench.Options, lookup func(string) string) {
	for _, p := range bench.Table {
		if !p.AppliesTo(opts.Mode) {
			continue
		}
		text := lookup(p.EnvName)
		if text == "" {
			continue
		}
		if value, err := p.Parse(text); err == nil {
			p.Apply(&opts.Workload, value)
		}
	}
}

// scenarioDoc is the top-level shape of a bench scenario file.
type scenarioDoc struct {
	Mode     string          `yaml:"mode"`
	Spec     yaml.RawMessage `yaml:"spec"`
	Workload yaml.RawMessage `yaml:"workload"`
}

// loadScenario layers a scenario file over the resolved options.
func loadScenario(path string, mode bench.Mode, opts *bench.Options) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("%w: %v", bench.ErrUsage, err)
	}
	var doc scenarioDoc
	if err := yaml.UnmarshalWithOptions(data, &doc, yaml.DisallowUnknownField()); err != nil {
		return fmt.Errorf("%w: %v", bench.ErrUsage, err)
	}
	if doc.Mode != "" && bench.Mode(doc.Mode) != mode {
		return fmt.Errorf("%w: scenario mode %q does not match %q", bench.ErrUsage, doc.Mode, mode)
	}
	if len(doc.Spec) > 0 {
		s, err := spec.DecodeSpecYAML(doc.Spec)
		if err != nil {
			return err
		}
		opts.Spec = s
	}
	if len(doc.Workload) > 0 {
		w, err := bench.DecodeWorkloadYAML(doc.Workload)
		if err != nil {
			return err
		}
		opts.Workload = w
	}
	return nil
}

// workloadValue binds one workload parameter to a flag.Value.
type workloadValue struct {
	param    bench.Param
	workload *bench.Workload
}

func (v *workloadValue) String() string { return "" }

func (v *workloadValue) Set(text string) error {
	value, err := v.param.Parse(text)
	if err != nil {
		return err
	}
	v.param.Apply(v.workload, value)
	return nil
}

// accessValue binds the access enum to a flag.Value.
type accessValue struct{ access *bench.Access }

func (v *accessValue) String() string { return string(*v.access) }

func (v *accessValue) Set(text string) error {
	switch bench.Access(text) {
	case bench.AccessEmbedded, bench.AccessDaemon, bench.AccessRemote:
		*v.access = bench.Access(text)
		return nil
	default:
		return fmt.Errorf("%w: access must be embedded, daemon, or remote", bench.ErrUsage)
	}
}

// benchPresentation picks the theme for a bench destination; the numbers are human-scaled either way.
func benchPresentation(w io.Writer) (style.Theme, human.Formatter) {
	if isTerminalWriter(w) {
		return style.NewTheme(true), human.Formatter{}
	}
	return style.PlainTheme(), human.Formatter{}
}

// splitCSV splits a comma-separated flag value into its trimmed parts.
func splitCSV(text string) []string {
	var out []string
	for _, part := range strings.Split(text, ",") {
		if part = strings.TrimSpace(part); part != "" {
			out = append(out, part)
		}
	}
	return out
}

// bench bar geometry and the warm end of the banner gradient.
const (
	progressWidth      = 24
	progressLabelWidth = 10
)

// benchBarColors fills the progress bar with the banner gradient's warm end.
var benchBarColors = style.Default.Gradient[:3]

// benchProgress draws a once-per-second progress bar for a running bench.
type benchProgress struct {
	mu      sync.Mutex
	w       io.Writer
	bar     progress.Model
	warmup  time.Duration
	measure time.Duration

	active   bench.Phase
	started  time.Time
	bytes    int64
	total    int64
	lineOpen bool

	done     chan struct{}
	stopOnce sync.Once
}

// startProgressPrinter starts a progress bar when errw is a terminal, and returns nil otherwise.
func startProgressPrinter(errw io.Writer, opts bench.Options) *benchProgress {
	if opts.Quiet || !isTerminalWriter(errw) {
		return nil
	}
	p := &benchProgress{
		w:       errw,
		warmup:  opts.Workload.Warmup,
		measure: opts.Workload.Duration,
		done:    make(chan struct{}),
		bar: progress.New(
			progress.WithWidth(progressWidth),
			progress.WithFillCharacters('=', '-'),
			progress.WithColors(benchBarColors...),
			progress.WithoutPercentage(),
		),
	}
	go p.loop()
	return p
}

// loop repaints the active phase every second until the printer stops.
func (p *benchProgress) loop() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.mu.Lock()
			if p.active != "" {
				p.repaintLocked()
			}
			p.mu.Unlock()
		}
	}
}

// stop ends the printer and closes the line it may have left open.
func (p *benchProgress) stop() {
	p.stopOnce.Do(func() { close(p.done) })
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.lineOpen {
		fmt.Fprintln(p.w)
		p.lineOpen = false
	}
}

// event renders one progress event.
func (p *benchProgress) event(e bench.ProgressEvent) {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch e.Kind {
	case bench.ProgressStart:
		p.active = e.Phase
		p.started = time.Now()
		p.bytes, p.total = e.Bytes, e.Total
		p.repaintLocked()
	case bench.ProgressEnd:
		p.bytes = max(p.bytes, e.Bytes)
		if p.total == 0 {
			p.total = e.Total
		}
		fmt.Fprint(p.w, "\r\x1b[2K", p.renderLocked(1))
		fmt.Fprintln(p.w)
		p.lineOpen = false
	case bench.ProgressTick:
		p.bytes = e.Bytes
		if e.Total > 0 {
			p.total = e.Total
		}
		p.repaintLocked()
	}
}

// repaintLocked redraws the current phase line in place.
func (p *benchProgress) repaintLocked() {
	if p.lineOpen {
		fmt.Fprint(p.w, "\r\x1b[2K")
	}
	fmt.Fprint(p.w, p.renderLocked(p.percentLocked()))
	p.lineOpen = true
}

// percentLocked estimates how far the active phase has progressed.
func (p *benchProgress) percentLocked() float64 {
	elapsed := time.Since(p.started).Seconds()
	switch {
	case p.total > 0:
		return clamp01(float64(p.bytes) / float64(p.total))
	case p.active == bench.PhaseMeasure && p.measure > 0:
		return clamp01(elapsed / p.measure.Seconds())
	case p.active == bench.PhaseWarmup && p.warmup > 0:
		return clamp01(elapsed / p.warmup.Seconds())
	default:
		return 0
	}
}

// renderLocked formats one progress line.
func (p *benchProgress) renderLocked(frac float64) string {
	label := fmt.Sprintf("[%s]", p.active)
	return fmt.Sprintf("%-*s %s %3d%%  %s",
		progressLabelWidth, label,
		p.bar.ViewAs(frac),
		int(frac*100+0.5),
		human.Formatter{}.Duration(time.Since(p.started)))
}

// clamp01 bounds v to the unit interval.
func clamp01(v float64) float64 { return min(max(v, 0), 1) }
