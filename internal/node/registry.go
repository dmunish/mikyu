package node

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/spec"
)

const (
	registryFileName = "queues.json"
	registryTmpName  = "queues.json.tmp"
	registryVersion  = 1
)

// ErrRegistryCorrupt reports a queues.json that could not be decoded.
var ErrRegistryCorrupt = errors.New("invalid queue registry")

// QueueEntry is the cached kind and sparse spec of one queue.
type QueueEntry struct {
	Name string       `json:"name"`
	Kind manager.Kind `json:"kind"`
	Spec spec.Spec    `json:"spec"`
}

type registry struct {
	Version int                   `json:"version"`
	Queues  map[string]QueueEntry `json:"queues"` // keyed by kind-qualified "kind/name"
}

// registryKey renders the composite identity of a queue in the registry.
func registryKey(kind manager.Kind, name string) string {
	return manager.Key{Name: name, Kind: kind}.Label()
}

// loadRegistry reads queues.json. A missing file yields an empty registry.
func loadRegistry(dir string) (*registry, error) {
	path := filepath.Join(dir, registryFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &registry{Version: registryVersion, Queues: map[string]QueueEntry{}}, nil
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
		r.Queues = map[string]QueueEntry{}
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
		r.Queues = map[string]QueueEntry{}
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer root.Close()

	f, err := root.OpenFile(registryTmpName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = root.Remove(registryTmpName)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = root.Remove(registryTmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = root.Remove(registryTmpName)
		return err
	}
	if err := root.Rename(registryTmpName, registryFileName); err != nil {
		return err
	}
	return engine.SyncDir(root)
}

// reconcileRegistry corrects the cache to the filesystem: entries whose
// directories are missing are purged, orphan directories are adopted with the
// kind their path declares, and a cached kind that disagrees with the path is
// rewritten to the path. It reports whether the registry changed.
func reconcileRegistry(reg *registry, infos []manager.QueueInfo) bool {
	onDisk := make(map[string]manager.QueueInfo, len(infos))
	for _, info := range infos {
		onDisk[registryKey(info.Kind, info.Name)] = info
	}
	changed := false
	for key, entry := range reg.Queues {
		if _, ok := onDisk[key]; !ok {
			delete(reg.Queues, key)
			changed = true
			slog.Warn("queue registry entry purged; directory missing", "queue", entry.Name, "kind", entry.Kind.String())
		}
	}
	for key, info := range onDisk {
		entry, ok := reg.Queues[key]
		if !ok {
			reg.Queues[key] = QueueEntry{Name: info.Name, Kind: info.Kind}
			changed = true
			slog.Warn("orphan queue adopted", "queue", info.Name, "kind", info.Kind.String())
			continue
		}
		if entry.Kind != info.Kind || entry.Name != info.Name {
			entry.Kind = info.Kind
			entry.Name = info.Name
			reg.Queues[key] = entry
			changed = true
			slog.Warn("queue registry corrected to path", "queue", info.Name, "kind", info.Kind.String())
		}
	}
	return changed
}