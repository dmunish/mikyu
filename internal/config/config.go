package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/goccy/go-yaml"
)

const defaultConfigYAML = `# Mikyu Daemon Configuration

server:
  target: "tcp://127.0.0.1:7420" # Bind target (tcp://, unix://, or pipe://)
  data_dir: "./data"             # Path for WAL log segments and offset store

engine:
  batch_size: 256                # Max pending messages before triggering WAL fsync
  flush_interval: "10ms"         # Max wait time before forcing WAL fsync
  max_segment_bytes: 67108864    # 64 MiB: Max size per log segment before rolling
  max_message_bytes: 1048576     # 1 MiB: Max payload size per produce call
  max_segment_age: "1h"          # Max time before rolling inactive segment

queues: []                       # Declarative pre-created queues on startup
`

type Config struct {
	Server ServerConfig  `yaml:"server"`
	Engine EngineConfig  `yaml:"engine"`
	Queues []QueueConfig `yaml:"queues"`
}

type ServerConfig struct {
	Target  string `yaml:"target"`
	DataDir string `yaml:"data_dir"`
}

type EngineConfig struct {
	BatchSize       int           `yaml:"batch_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes"`
	MaxMessageBytes int64         `yaml:"max_message_bytes"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age"`
}

type QueueConfig struct {
	Name            string        `yaml:"name"`
	BatchSize       int           `yaml:"batch_size,omitempty"`
	FlushInterval   time.Duration `yaml:"flush_interval,omitempty"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes,omitempty"`
	MaxMessageBytes int64         `yaml:"max_message_bytes,omitempty"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age,omitempty"`
}

func DefaultConfig() Config {
	return Config{
		Server: ServerConfig{
			Target:  "tcp://127.0.0.1:7420",
			DataDir: "./data",
		},
		Engine: EngineConfig{
			BatchSize:       256,
			FlushInterval:   10 * time.Millisecond,
			MaxSegmentBytes: 64 * 1024 * 1024,
			MaxMessageBytes: 1 * 1024 * 1024,
			MaxSegmentAge:   1 * time.Hour,
		},
		Queues: []QueueConfig{},
	}
}

// Load resolves the config file using the resolution hierarchy,
// bootstraps a default file if missing, parses YAML via goccy/go-yaml,
// and applies environment variable overrides.
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

	// Use goccy/go-yaml unmarshaler
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, "", fmt.Errorf("failed to parse yaml config at %s: %w", resolvedPath, err)
	}

	applyEnvOverrides(&cfg)

	return &cfg, resolvedPath, nil
}

func resolveOrBootstrapPath(flagPath string) (path string, bootstrapped bool, err error) {
	// 1. Explicit CLI Flag
	if flagPath != "" {
		if _, err := os.Stat(flagPath); err == nil {
			return flagPath, false, nil
		}
		return "", false, fmt.Errorf("specified config file does not exist: %s", flagPath)
	}

	// 2. Current working directory
	localPath := "./mikyu.yaml"
	if _, err := os.Stat(localPath); err == nil {
		return localPath, false, nil
	}

	// 3. User config directory (~/.config/mikyu/mikyu.yaml)
	userConfigDir, err := os.UserConfigDir()
	if err == nil {
		userPath := filepath.Join(userConfigDir, "mikyu", "mikyu.yaml")
		if _, err := os.Stat(userPath); err == nil {
			return userPath, false, nil
		}

		// Bootstrap default config in user config directory
		if err := os.MkdirAll(filepath.Dir(userPath), 0o755); err == nil {
			if err := os.WriteFile(userPath, []byte(defaultConfigYAML), 0o644); err == nil {
				return userPath, true, nil
			}
		}
	}

	// 4. Fallback: Bootstrap in local directory
	if err := os.WriteFile(localPath, []byte(defaultConfigYAML), 0o644); err == nil {
		return localPath, true, nil
	}

	return "", false, fmt.Errorf("unable to resolve or bootstrap config file")
}

func applyEnvOverrides(cfg *Config) {
	if val := os.Getenv("MIKYU_SERVER_TARGET"); val != "" {
		cfg.Server.Target = val
	}
	if val := os.Getenv("MIKYU_SERVER_DATA_DIR"); val != "" {
		cfg.Server.DataDir = val
	}
	if val := os.Getenv("MIKYU_ENGINE_BATCH_SIZE"); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			cfg.Engine.BatchSize = n
		}
	}
	if val := os.Getenv("MIKYU_ENGINE_FLUSH_INTERVAL"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			cfg.Engine.FlushInterval = d
		}
	}
	if val := os.Getenv("MIKYU_ENGINE_MAX_SEGMENT_BYTES"); val != "" {
		if bytes, err := strconv.ParseInt(val, 10, 64); err == nil && bytes > 0 {
			cfg.Engine.MaxSegmentBytes = bytes
		}
	}
	if val := os.Getenv("MIKYU_ENGINE_MAX_MESSAGE_BYTES"); val != "" {
		if bytes, err := strconv.ParseInt(val, 10, 64); err == nil && bytes > 0 {
			cfg.Engine.MaxMessageBytes = bytes
		}
	}
	if val := os.Getenv("MIKYU_ENGINE_MAX_SEGMENT_AGE"); val != "" {
		if d, err := time.ParseDuration(val); err == nil && d > 0 {
			cfg.Engine.MaxSegmentAge = d
		}
	}
}

func ToEngineConfig(e EngineConfig) engine.Config {
	return engine.Config{
		BatchSize:       e.BatchSize,
		Interval:        e.FlushInterval,
		MaxSegmentBytes: e.MaxSegmentBytes,
		MaxMessageBytes: e.MaxMessageBytes,
		MaxSegmentAge:   e.MaxSegmentAge,
	}
}

func ToQueueEngineConfig(defaults EngineConfig, q QueueConfig) engine.Config {
	cfg := ToEngineConfig(defaults)
	if q.BatchSize > 0 {
		cfg.BatchSize = q.BatchSize
	}
	if q.FlushInterval > 0 {
		cfg.Interval = q.FlushInterval
	}
	if q.MaxSegmentBytes > 0 {
		cfg.MaxSegmentBytes = q.MaxSegmentBytes
	}
	if q.MaxMessageBytes > 0 {
		cfg.MaxMessageBytes = q.MaxMessageBytes
	}
	if q.MaxSegmentAge > 0 {
		cfg.MaxSegmentAge = q.MaxSegmentAge
	}
	return cfg
}
