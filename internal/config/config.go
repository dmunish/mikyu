package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/goccy/go-yaml"
)

const defaultConfigYAML = `# Mikyu Daemon Configuration

server:
  target: "tcp://127.0.0.1:7420" # Bind target (tcp://, unix://, or pipe://)
  data_dir: "./data"             # Path for WAL log segments and offset store

defaults:                        # Session defaults inherited by new queues
  engine:
    batch_size: 256              # Max pending messages before triggering WAL fsync
    max_interval: "10ms"         # Max wait time before forcing WAL fsync
    min_interval: "30us"         # Micro-linger window to group concurrent produces
    segment_size: 67108864       # 64 MiB: Max size per log segment before rolling
    max_message_bytes: 1048576   # 1 MiB: Max payload size per produce call
    segment_age: "1h"            # Max time before rolling inactive segment
    encrypted: false             # Enable OS transparent directory encryption
    fsync: true                  # Perform physical disk fsync on produce

queues: {}                       # Declarative pre-created queues on startup
`

// Config is the loader's view of mikyu.yaml: one schema, two files.
type Config struct {
	Server   ServerConfig         `yaml:"server"`
	Defaults spec.Spec            `yaml:"defaults"`
	Queues   map[string]spec.Spec `yaml:"queues"`
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
		Queues: map[string]spec.Spec{},
	}
}

// Load reads mikyu.yaml, applies MIKYU_* environment overrides, and returns the resolved config plus the path it read (bootstrapping a default file if none exists).
func Load(flagPath string) (*Config, string, error) {
	cfg := DefaultConfig()
	resolvedPath, bootstrapped, err := resolveOrBootstrapPath(flagPath)
	if err != nil {
		return nil, "", fmt.Errorf("config path resolution failed: %w", err)
	}

	if bootstrapped {
		log.Printf("No config file found. Bootstrapped default config at %s", resolvedPath)
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
		cfg.Queues = map[string]spec.Spec{}
	}

	if err := applyEnvOverrides(&cfg); err != nil {
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

	localPath := "./mikyu.yaml"
	if _, err := os.Stat(localPath); err == nil {
		return localPath, false, nil
	}

	userConfigDir, err := os.UserConfigDir()
	if err == nil {
		userPath := filepath.Join(userConfigDir, "mikyu", "mikyu.yaml")
		if _, err := os.Stat(userPath); err == nil {
			return userPath, false, nil
		}

		if err := os.MkdirAll(filepath.Dir(userPath), 0o755); err == nil {
			if err := os.WriteFile(userPath, []byte(defaultConfigYAML), 0o644); err == nil {
				return userPath, true, nil
			}
		}
	}

	if err := os.WriteFile(localPath, []byte(defaultConfigYAML), 0o644); err == nil {
		return localPath, true, nil
	}

	return "", false, fmt.Errorf("unable to resolve or bootstrap config file")
}

// legacyYAMLKeys maps pre-re-root keys to their canonical replacements.
var legacyYAMLKeys = map[string]string{
	"max_segment_bytes": "segment_size",
	"max_segment_age":   "segment_age",
}

func checkLegacyKeys(data []byte) error {
	for old, next := range legacyYAMLKeys {
		if strings.Contains(string(data), old) {
			return fmt.Errorf("%s was renamed to %s: update mikyu.yaml", old, next)
		}
	}
	return nil
}

// legacyEnvVars maps pre-re-root environment names to their canonical replacements.
var legacyEnvVars = map[string]string{
	"MIKYU_SERVER_TARGET":            "MIKYU_TARGET",
	"MIKYU_SERVER_DATA_DIR":          "MIKYU_DATA_DIR",
	"MIKYU_ENGINE_BATCH_SIZE":        "MIKYU_BATCH_SIZE",
	"MIKYU_ENGINE_MAX_INTERVAL":      "MIKYU_MAX_INTERVAL",
	"MIKYU_ENGINE_FLUSH_INTERVAL":    "MIKYU_MAX_INTERVAL",
	"MIKYU_ENGINE_MIN_INTERVAL":      "MIKYU_MIN_INTERVAL",
	"MIKYU_ENGINE_SEGMENT_SIZE":      "MIKYU_SEGMENT_SIZE",
	"MIKYU_ENGINE_MAX_MESSAGE_BYTES": "MIKYU_MAX_MESSAGE_BYTES",
	"MIKYU_ENGINE_SEGMENT_AGE":       "MIKYU_SEGMENT_AGE",
	"MIKYU_ENGINE_ENCRYPTED":         "MIKYU_ENCRYPTED",
	"MIKYU_ENGINE_FSYNC":             "MIKYU_FSYNC",
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
