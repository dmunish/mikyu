package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/dmunish/mikyu/internal/manager"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/goccy/go-yaml"
)

const defaultConfigYAML = `# Mikyu Daemon Configuration

server:
  target: "tcp://127.0.0.1:7420" # Bind target (tcp://, unix://, or pipe://)
  data_dir: "./data"             # Path for WAL log segments and offset store
  max_connections: 10            # Connection pool size

defaults:                        # Session defaults inherited by new queues
  storage:
    durability: sync             # sync fsyncs before the offset returns; async flushes in the background
    encryption: none             # none, os for OS directory encryption, or aes for sealed payloads
    compaction: none             # none, or keys to drop superseded keyed records at segment roll
    max_segment: 67108864        # 64 MiB: bytes per segment file before rolling
    max_segment_age: "1h"        # Max time before rolling the active segment
    max_payload: 1048576         # 1 MiB: largest payload an append accepts (derived when unset)
    max_cache: 33554432          # 32 MiB: tail cache budget in bytes (derived when unset)
  flush:
    max_bytes: 1048576           # Flush once the pending WAL reaches this many bytes
    max_wait: "10ms"             # Longest a batch waits before flushing
  retention:
    max_age: "0s"                # Drop closed segments older than this; 0 keeps forever
    max_bytes: 0                 # Keep at most this many bytes of history; 0 keeps forever
  task:
    lease: "30s"                 # Lease length assigned to a dequeued task
    max_attempts: 5              # Deliveries before a task is dead-lettered
    max_in_flight: 100000        # Concurrent-lease upper bound
    max_dead_letters: 100000     # Dead-letter upper bound (derived when unset)

queues: {}                       # Declarative pre-created queues on startup
`

// QueueConfig declares one pre-created queue: its kind and its sparse spec.
type QueueConfig struct {
	Kind manager.Kind `yaml:"kind"`
	Spec spec.Spec    `yaml:"spec"`
}

// Config is the loader's view of mikyu.yaml: one schema, two files.
type Config struct {
	Server   ServerConfig           `yaml:"server"`
	Defaults spec.Spec              `yaml:"defaults"`
	Queues   map[string]QueueConfig `yaml:"queues"`
}

// ServerConfig is the process-level server block, identical to the spec session type.
type ServerConfig = spec.Session

// DefaultConfig returns the compiled defaults.
func DefaultConfig() Config {
	sess := spec.BuiltinSession()
	if sess.DataDir == "" {
		sess.DataDir = "./data"
	}
	return Config{
		Server: sess,
		Queues: map[string]QueueConfig{},
	}
}

// configFileName is the default config file name, resolved next to the executable.
const configFileName = "mikyu.yaml"

// executablePath resolves the running binary for config bootstrap; tests override it.
var executablePath = os.Executable

// Load reads the config file (the --config path, or configFileName next to the executable), applies MIKYU_* environment overrides, and returns the resolved config plus the path it read (bootstrapping the default file next to the executable if none exists).
func Load(flagPath string) (*Config, string, error) {
	cfg := DefaultConfig()
	resolvedPath, bootstrapped, err := resolveOrBootstrapPath(flagPath)
	if err != nil {
		return nil, "", fmt.Errorf("config path resolution failed: %w", err)
	}

	if bootstrapped {
		slog.Info("no config file found; bootstrapped the default config", "path", resolvedPath)
	}

	data, err := os.ReadFile(resolvedPath)
	if err != nil {
		return nil, "", fmt.Errorf("failed to read config file %s: %w", resolvedPath, err)
	}

	if err := checkLegacyKeys(data); err != nil {
		return nil, "", err
	}

	if err := yaml.UnmarshalWithOptions(data, &cfg, yaml.DisallowUnknownField()); err != nil {
		return nil, "", fmt.Errorf("failed to parse yaml config at %s: %w", resolvedPath, err)
	}

	if cfg.Queues == nil {
		cfg.Queues = map[string]QueueConfig{}
	}

	if err := applyEnvOverrides(&cfg); err != nil {
		return nil, "", err
	}

	if err := spec.ValidateSession(cfg.Server); err != nil {
		return nil, "", err
	}
	if err := spec.ValidateCreate(cfg.Defaults); err != nil {
		return nil, "", err
	}

	return &cfg, resolvedPath, nil
}

func resolveOrBootstrapPath(flagPath string) (path string, bootstrapped bool, err error) {
	if flagPath != "" {
		if _, err := os.Stat(flagPath); err == nil {
			return flagPath, false, nil
		}
		return "", false, fmt.Errorf("specified config file does not exist: %s", flagPath)
	}

	exePath, err := executablePath()
	if err != nil {
		return "", false, fmt.Errorf("unable to resolve executable directory for config bootstrap: %w", err)
	}
	path = filepath.Join(filepath.Dir(exePath), configFileName)

	if _, err := os.Stat(path); err == nil {
		return path, false, nil
	}
	if err := os.WriteFile(path, []byte(defaultConfigYAML), 0o644); err != nil {
		return "", false, fmt.Errorf(
			"unable to bootstrap config file at %s: %w (pass --config <path> to specify an alternate location)",
			path, err,
		)
	}
	return path, true, nil
}

// legacyYAMLKeys maps pre-redesign keys to their canonical replacements. Keys
// that remain canonical under the new schema (compaction, max_attempts) are
// absent: their old values fail type or enum validation on their own.
var legacyYAMLKeys = map[string]string{
	"max_segment_bytes":    "max_segment",
	"batch_size":           "max_flush_bytes",
	"max_interval":         "max_flush_wait",
	"min_interval":         "max_flush_wait (the linger is now derived)",
	"segment_size":         "max_segment",
	"segment_age":          "max_segment_age",
	"max_message_bytes":    "max_payload",
	"fsync":                "durability",
	"encrypted":            "encryption",
	"max_conns":            "max_connections",
	"max_dlq":              "max_dead_letters",
	"default_visibility":   "lease",
	"default_max_attempts": "max_attempts",
	"retention_age":        "max_retention_age",
	"retention_bytes":      "max_retention_bytes",
}

// legacyKeyPattern matches a legacy YAML key at key position, so new keys that
// contain a legacy name ("max_segment_age" contains "segment_age") never trip.
var legacyKeyPattern = func() *regexp.Regexp {
	names := make([]string, 0, len(legacyYAMLKeys))
	for old := range legacyYAMLKeys {
		names = append(names, regexp.QuoteMeta(old))
	}
	slices.Sort(names)
	return regexp.MustCompile(`(?m)^[ \t-]*(` + strings.Join(names, "|") + `)[ \t]*:`)
}()

func checkLegacyKeys(data []byte) error {
	m := legacyKeyPattern.FindSubmatch(data)
	if m == nil {
		return nil
	}
	old := string(m[1])
	return fmt.Errorf("%s was renamed to %s: update mikyu.yaml", old, legacyYAMLKeys[old])
}

// legacyEnvVars maps pre-redesign environment names to their canonical replacements.
var legacyEnvVars = map[string]string{
	"MIKYU_SERVER_TARGET":            "MIKYU_TARGET",
	"MIKYU_SERVER_DATA_DIR":          "MIKYU_DATA_DIR",
	"MIKYU_ENGINE_BATCH_SIZE":        "MIKYU_MAX_FLUSH_BYTES",
	"MIKYU_ENGINE_MAX_INTERVAL":      "MIKYU_MAX_FLUSH_WAIT",
	"MIKYU_ENGINE_FLUSH_INTERVAL":    "MIKYU_MAX_FLUSH_WAIT",
	"MIKYU_ENGINE_MIN_INTERVAL":      "MIKYU_MAX_FLUSH_WAIT",
	"MIKYU_ENGINE_SEGMENT_SIZE":      "MIKYU_MAX_SEGMENT",
	"MIKYU_ENGINE_MAX_MESSAGE_BYTES": "MIKYU_MAX_PAYLOAD",
	"MIKYU_ENGINE_SEGMENT_AGE":       "MIKYU_MAX_SEGMENT_AGE",
	"MIKYU_ENGINE_ENCRYPTED":         "MIKYU_ENCRYPTION",
	"MIKYU_ENGINE_FSYNC":             "MIKYU_DURABILITY",
	"MIKYU_BATCH_SIZE":               "MIKYU_MAX_FLUSH_BYTES (unit changed: bytes, not records)",
	"MIKYU_MAX_INTERVAL":             "MIKYU_MAX_FLUSH_WAIT",
	"MIKYU_MIN_INTERVAL":             "MIKYU_MAX_FLUSH_WAIT (the linger is now derived)",
	"MIKYU_SEGMENT_SIZE":             "MIKYU_MAX_SEGMENT",
	"MIKYU_MAX_MESSAGE_BYTES":        "MIKYU_MAX_PAYLOAD",
	"MIKYU_SEGMENT_AGE":              "MIKYU_MAX_SEGMENT_AGE",
	"MIKYU_ENCRYPTED":                "MIKYU_ENCRYPTION",
	"MIKYU_FSYNC":                    "MIKYU_DURABILITY",
	"MIKYU_MAX_CONNS":                "MIKYU_MAX_CONNECTIONS",
	"MIKYU_MAX_DLQ":                  "MIKYU_MAX_DEAD_LETTERS",
	"MIKYU_DEFAULT_VISIBILITY":       "MIKYU_LEASE",
	"MIKYU_DEFAULT_MAX_ATTEMPTS":     "MIKYU_MAX_ATTEMPTS",
	"MIKYU_RETENTION_AGE":            "MIKYU_MAX_RETENTION_AGE",
	"MIKYU_RETENTION_BYTES":          "MIKYU_MAX_RETENTION_BYTES",
}

// applyEnvOverrides layers MIKYU_<CANONICAL_KEY> over the loaded file, driven
// entirely by spec.Table: session parameters fill the server block, spec
// parameters fill the session defaults.
func applyEnvOverrides(cfg *Config) error {
	for legacy, next := range legacyEnvVars {
		if os.Getenv(legacy) != "" {
			return fmt.Errorf("%s is no longer supported: use %s", legacy, next)
		}
	}
	if err := spec.ApplyEnvSession(&cfg.Server, os.Getenv); err != nil {
		return err
	}
	return spec.ApplyEnv(&cfg.Defaults, os.Getenv)
}
