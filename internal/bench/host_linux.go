//go:build linux

package bench

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"golang.org/x/sys/unix"
)

// rusageRSSUnit is getrusage's Maxrss unit in bytes on Linux.
const rusageRSSUnit int64 = 1024

// readPlatform reads RSS and CPU time from getrusage and the rest from /proc.
func readPlatform() HostStats {
	rss, cpuNanos := rusageStats()
	stats := HostStats{RSSBytes: rss, CPUPercent: cpuPercentDelta(cpuNanos)}
	if data, err := os.ReadFile("/proc/self/stat"); err == nil {
		if fields := strings.Fields(string(data)); len(fields) >= 23 {
			if v, err := strconv.ParseInt(fields[22], 10, 64); err == nil && v >= 0 {
				stats.VSZBytes = v
			}
		}
	}
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		if v, ok := procStatusValue(string(data), "Threads:"); ok {
			stats.Threads = v
		}
	}
	if entries, err := os.ReadDir("/proc/self/fd"); err == nil {
		stats.OpenFDs = int64(len(entries))
	}
	return stats
}

// procStatusValue reads one integer field from a /proc status document.
func procStatusValue(status, key string) (int64, bool) {
	for line := range strings.SplitSeq(status, "\n") {
		if value, ok := strings.CutPrefix(line, key); ok {
			v, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			return v, err == nil && v >= 0
		}
	}
	return 0, false
}

// dropCache evicts the page cache for every file under dir and reports whether a reread faulted.
func dropCache(dir string) (bool, error) {
	largest, err := dropFiles(dir)
	if err != nil {
		return false, err
	}
	if largest == "" {
		return false, nil
	}
	before, err := procMajorFaults()
	if err != nil {
		return false, nil
	}
	if err := readAndDiscard(largest); err != nil {
		return false, err
	}
	after, err := procMajorFaults()
	if err != nil {
		return false, nil
	}
	return after > before, nil
}

// dropFiles fadvises every file under dir and returns the largest one.
func dropFiles(dir string) (string, error) {
	var largest string
	var largestSize int64
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if info.Size() > largestSize {
			largest, largestSize = path, info.Size()
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		adviseErr := unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		_ = f.Close()
		return adviseErr
	})
	return largest, err
}

// readAndDiscard reads a file once into a reused buffer to force a cache miss.
func readAndDiscard(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, 64<<10)
	for {
		if _, err := f.Read(buf); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// procMajorFaults reads the process's cumulative major page faults from /proc/self/stat.
func procMajorFaults() (int64, error) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, err
	}
	closing := strings.LastIndexByte(string(data), ')')
	if closing < 0 || closing+2 >= len(data) {
		return 0, io.ErrUnexpectedEOF
	}
	fields := strings.Fields(string(data)[closing+2:])
	if len(fields) < 10 {
		return 0, io.ErrUnexpectedEOF
	}
	return strconv.ParseInt(fields[9], 10, 64)
}
