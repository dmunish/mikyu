package result

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
)

var (
	// ErrNotARun reports a directory without a bench envelope in it.
	ErrNotARun = errors.New("not a bench run")
	// ErrEnvelopeVersion reports an envelope this build cannot read.
	ErrEnvelopeVersion = errors.New("unsupported envelope version")
)

// ReadResult reads and validates <dir>/result.json.
func ReadResult(dir string) (Result, error) {
	data, err := os.ReadFile(filepath.Join(dir, resultFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Result{}, ErrNotARun
		}
		return Result{}, err
	}
	var r Result
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return Result{}, err
	}
	if r.Version != CurrentVersion {
		return Result{}, ErrEnvelopeVersion
	}
	return r, nil
}

// ReadSeries reads every observation from <dir>/series.csv.
func ReadSeries(dir string) ([]SeriesRow, error) {
	file, err := os.Open(filepath.Join(dir, seriesFileName))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, ErrNotARun
		}
		return nil, err
	}
	defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(records) < 2 {
		return nil, nil
	}
	rows := make([]SeriesRow, 0, len(records)-1)
	for _, rec := range records[1:] {
		row, err := parseRow(rec)
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// IsRun reports whether dir holds both envelope files.
func IsRun(dir string) bool {
	for _, name := range []string{resultFileName, seriesFileName} {
		if _, err := os.Stat(filepath.Join(dir, name)); err != nil {
			return false
		}
	}
	return true
}

func parseRow(rec []string) (SeriesRow, error) {
	if len(rec) != 19 {
		return SeriesRow{}, errors.New("result: series row has " + strconv.Itoa(len(rec)) + " columns, want 19")
	}
	var r SeriesRow
	r.T, _ = strconv.ParseFloat(rec[0], 64)
	r.Phase = rec[1]
	r.Client = ClientCols{
		MsgsInFlight: num(rec[2]),
		Throughput:   flt(rec[3]),
		P50NS:        num(rec[4]),
		P90NS:        num(rec[5]),
		P99NS:        num(rec[6]),
	}
	r.System = SystemCols{
		QueueDepth:     num(rec[7]),
		Segments:       num(rec[8]),
		SizeBytes:      num(rec[9]),
		CacheHits:      num(rec[10]),
		CacheMisses:    num(rec[11]),
		CacheBytes:     num(rec[12]),
		WALChannelFill: flt(rec[13]),
		WALByteFill:    flt(rec[14]),
	}
	r.Host = HostCols{
		RSSBytes:   num(rec[15]),
		CPUPercent: flt(rec[16]),
		OpenFDs:    num(rec[17]),
		Threads:    num(rec[18]),
	}
	return r, nil
}

func num(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

func flt(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}
