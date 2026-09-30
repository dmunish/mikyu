package result

import (
	"encoding/json"
	"os"
	"strconv"
)

const (
	resultFileName = "result.json"
	resultTmpName  = ".result.json.tmp"
	seriesFileName = "series.csv"
)

// WriteResult writes r to <dir>/result.json atomically, fsyncing the file and its directory.
func WriteResult(dir string, r Result) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	data, err := marshalIndent(r)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()
	return replaceFile(root, resultTmpName, resultFileName, data)
}

// SeriesWriter appends observations to <dir>/series.csv, one write per row.
type SeriesWriter struct {
	file *os.File
	row  []byte
}

// OpenSeries opens <dir>/series.csv and writes its header row; the caller closes the writer.
func OpenSeries(dir string) (*SeriesWriter, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer root.Close()

	file, err := root.OpenFile(seriesFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if _, err := file.WriteString(seriesHeader); err != nil {
		_ = file.Close()
		return nil, err
	}
	return &SeriesWriter{file: file, row: make([]byte, 0, 256)}, nil
}

// Append writes one observation with a single unbuffered write, so a kill loses nothing already written.
func (w *SeriesWriter) Append(r SeriesRow) error {
	b := strconv.AppendFloat(w.row[:0], r.T, 'f', 3, 64)
	b = append(b, ',')
	b = append(b, r.Phase...)
	b = appendI64(b, r.Client.MsgsInFlight)
	b = appendF64(b, r.Client.Throughput)
	b = appendI64(b, r.Client.P50NS)
	b = appendI64(b, r.Client.P90NS)
	b = appendI64(b, r.Client.P99NS)
	b = appendI64(b, r.System.QueueDepth)
	b = appendI64(b, r.System.Segments)
	b = appendI64(b, r.System.SizeBytes)
	b = appendI64(b, r.System.CacheHits)
	b = appendI64(b, r.System.CacheMisses)
	b = appendI64(b, r.System.CacheBytes)
	b = appendF64(b, r.System.WALChannelFill)
	b = appendF64(b, r.System.WALByteFill)
	b = appendI64(b, r.Host.RSSBytes)
	b = appendF64(b, r.Host.CPUPercent)
	b = appendI64(b, r.Host.OpenFDs)
	b = appendI64(b, r.Host.Threads)
	w.row = append(b, '\n')

	_, err := w.file.Write(w.row)
	return err
}

// Close closes the series file.
func (w *SeriesWriter) Close() error { return w.file.Close() }

func appendI64(b []byte, v int64) []byte {
	b = append(b, ',')
	return strconv.AppendInt(b, v, 10)
}

func appendF64(b []byte, v float64) []byte {
	b = append(b, ',')
	return strconv.AppendFloat(b, v, 'f', 3, 64)
}

// marshalIndent renders a document the way the envelope files store it: two-space indent, trailing newline.
func marshalIndent(v any) ([]byte, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

// replaceFile writes data to tmp inside root, fsyncs it, renames it over final, then fsyncs the directory.
func replaceFile(root *os.Root, tmp, final string, data []byte) error {
	f, err := root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		return abortWrite(root, f, tmp, err)
	}
	if err := f.Sync(); err != nil {
		return abortWrite(root, f, tmp, err)
	}
	if err := f.Close(); err != nil {
		_ = root.Remove(tmp)
		return err
	}
	if err := root.Rename(tmp, final); err != nil {
		return err
	}
	return syncDirOS(root)
}

// abortWrite closes a half-written temp file and clears it before reporting the failure.
func abortWrite(root *os.Root, f *os.File, tmp string, cause error) error {
	_ = f.Close()
	_ = root.Remove(tmp)
	return cause
}
