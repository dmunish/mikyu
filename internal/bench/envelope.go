package bench

import (
	"errors"
	"time"

	"github.com/dmunish/mikyu/internal/bench/result"
)

// envelope streams one run's result.json and series.csv to disk.
type envelope struct {
	dir    string
	r      result.Result
	series *result.SeriesWriter
	notes  []string
}

// newEnvelope writes the incomplete envelope and opens the series stream.
func newEnvelope(dir string, r result.Result) (*envelope, error) {
	r.Complete = false
	r.Aborted = ""
	if err := result.WriteResult(dir, r); err != nil {
		return nil, err
	}
	series, err := result.OpenSeries(dir)
	if err != nil {
		return nil, err
	}
	return &envelope{dir: dir, r: r, series: series}, nil
}

// AppendRow writes one series observation.
func (e *envelope) AppendRow(row result.SeriesRow) error { return e.series.Append(row) }

// AddNote records a note for the final result.json.
func (e *envelope) AddNote(note string) { e.notes = append(e.notes, note) }

// result returns the envelope's current document.
func (e *envelope) result() result.Result { return e.r }

// Finish rewrites result.json with the final metrics and closes the series stream.
func (e *envelope) Finish(m *result.MetricsView, phases []result.PhaseView, aborted string) error {
	e.r.Metrics = m
	e.r.Phases = phases
	e.r.Notes = e.notes
	e.r.EndedAt = time.Now()
	if aborted == "" {
		e.r.Complete = true
	} else {
		e.r.Aborted = aborted
	}
	return errors.Join(result.WriteResult(e.dir, e.r), e.series.Close())
}
