package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dmunish/mikyu/internal/spec"
)

const (
	registryFileName = "queues.json"
	registryTmpName  = "queues.json.tmp"
	registryVersion  = 1
)

// ErrRegistryCorrupt reports a queues.json that could not be decoded.
var ErrRegistryCorrupt = errors.New("invalid queue registry")

type registry struct {
	Version int                  `json:"version"`
	Queues  map[string]spec.Spec `json:"queues"`
}

// loadRegistry reads queues.json. A missing file yields an empty registry.
func loadRegistry(dir string) (*registry, error) {
	path := filepath.Join(dir, registryFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &registry{Version: registryVersion, Queues: map[string]spec.Spec{}}, nil
		}
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var r registry
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&r); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrRegistryCorrupt, path, err)
	}
	if r.Queues == nil {
		r.Queues = map[string]spec.Spec{}
	}
	if r.Version == 0 {
		r.Version = registryVersion
	}
	return &r, nil
}

// saveRegistry atomically replaces queues.json via a temp file + rename.
func saveRegistry(dir string, r *registry) error {
	if r.Version == 0 {
		r.Version = registryVersion
	}
	if r.Queues == nil {
		r.Queues = map[string]spec.Spec{}
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, registryTmpName)
	final := filepath.Join(dir, registryFileName)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, final)
}

// reconcileRegistry purges entries whose directories are missing; existing
// directories without an entry are adopted as orphan queues on session
// defaults.
func reconcileRegistry(dir string, reg *registry) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	existing := make(map[string]bool)
	for _, e := range entries {
		if e.IsDir() {
			existing[e.Name()] = true
		}
	}
	purged := false
	for name := range reg.Queues {
		if !existing[name] {
			delete(reg.Queues, name)
			purged = true
			slog.Warn("queue registry entry purged; directory missing", "queue", name)
		}
	}
	if !purged {
		return nil
	}
	return saveRegistry(dir, reg)
}
