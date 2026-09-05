// 设计依据：docs/dev/代码组织方案.md §4「包结构」
//                docs/技术设计方案.md §7.4「密钥和脱敏」

// Package config loads process configuration from a YAML file and resolves
// secret references.
//
// Configuration is split in two: this file holds infrastructure settings that
// belong to a deployment (addresses, pool sizes, timeouts), while tenant and
// agent configuration lives in the control-plane database and is read at
// request time. Putting tenant configuration here would make the platform
// single-tenant no matter what the database says.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole process configuration. Both Gateway and Worker read the
// same file; each uses the sections it needs.
type Config struct {
	Gateway   GatewayConfig   `yaml:"gateway"`
	Worker    WorkerConfig    `yaml:"worker"`
	MySQL     MySQLConfig     `yaml:"mysql"`
	Redis     RedisConfig     `yaml:"redis"`
	Scheduler SchedulerConfig `yaml:"scheduler"`
	Runtime   RuntimeConfig   `yaml:"runtime"`
	Storage   StorageConfig   `yaml:"storage"`
	Secrets   SecretsConfig   `yaml:"secrets"`
	Models    []ModelConfig   `yaml:"models"`
	Log       LogConfig       `yaml:"log"`
	Telemetry TelemetryConfig `yaml:"telemetry"`
}

// GatewayConfig configures the inbound HTTP process.
type GatewayConfig struct {
	Addr            string        `yaml:"addr"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// WorkerConfig configures the queue-consuming process.
type WorkerConfig struct {
	// ID identifies this worker as a lease owner. Empty means derive it from
	// hostname and pid, which is what keeps two workers on one machine from
	// stealing each other's leases.
	ID              string        `yaml:"id"`
	Concurrency     int           `yaml:"concurrency"`
	ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

// MySQLConfig configures the control-plane and business database.
type MySQLConfig struct {
	DSN             string        `yaml:"dsn"`
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
}

// RedisConfig configures runtime state: session state, leases, mailboxes and
// the queue.
type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

// SchedulerConfig tunes session ordering and failure recovery.
type SchedulerConfig struct {
	// LeaseTTL is also the upper bound on failure recovery: a worker that
	// dies holding a lease blocks its session for at most this long. Too
	// short and long-running work loses its lease mid-flight; too long and
	// recovery drags.
	LeaseTTL time.Duration `yaml:"lease_ttl"`

	// LeaseRenewInterval must be well below LeaseTTL.
	LeaseRenewInterval time.Duration `yaml:"lease_renew_interval"`

	// MaxMessageAttempts bounds retries of one message. Past the bound the
	// message goes to the dead letter and the mailbox keeps draining, so a
	// single poison message cannot block a session forever.
	MaxMessageAttempts int `yaml:"max_message_attempts"`

	// QueueKey is the Redis list backing the dispatcher.
	QueueKey string `yaml:"queue_key"`
}

// RuntimeConfig bounds the assembled-runtime cache.
type RuntimeConfig struct {
	CacheSize int           `yaml:"cache_size"`
	IdleTTL   time.Duration `yaml:"idle_ttl"`
}

// StorageConfig describes the backends and how data types map onto them.
// Phase one reads this mapping from the file; phase two reads it from
// backend_configs and backend_bindings without changing the router interface.
type StorageConfig struct {
	Backends []BackendConfig   `yaml:"backends"`
	Defaults map[string]string `yaml:"defaults"`
	Rules    []StorageRule     `yaml:"rules"`
}

// BackendConfig is one configured backend instance.
type BackendConfig struct {
	Name   string `yaml:"name"`
	Kind   string `yaml:"kind"`
	DSNRef string `yaml:"dsn_ref"`
}

// StorageRule overrides the default backend for one tenant, agent and data
// type.
type StorageRule struct {
	TenantID   string `yaml:"tenant_id"`
	AgentAppID string `yaml:"agent_app_id"`
	DataType   string `yaml:"data_type"`
	Backend    string `yaml:"backend"`
}

// SecretsConfig resolves secret:// references to actual values. The database
// only ever stores the reference.
type SecretsConfig struct {
	// Resolver selects the backing store. Phase one supports "env".
	Resolver string `yaml:"resolver"`
	// Mapping maps a secret:// reference to an environment variable name.
	Mapping map[string]string `yaml:"mapping"`
}

// ModelConfig is one model endpoint. DeepSeek and similar providers speak the
// OpenAI protocol, so they share one client.
type ModelConfig struct {
	Name    string `yaml:"name"`
	BaseURL string `yaml:"base_url"`
}

// LogConfig configures log output.
type LogConfig struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// TelemetryConfig configures OpenTelemetry export.
//
// Enabled gates only the exporter. Trace context still propagates across the
// queue when it is false, so switching export on in production is not the
// first time that plumbing runs.
type TelemetryConfig struct {
	Enabled bool `yaml:"enabled"`
	// Endpoint is the OTLP collector address. Empty falls back to the
	// OTEL_EXPORTER_OTLP_ENDPOINT environment variable.
	Endpoint string `yaml:"endpoint"`
	// Protocol is grpc or http.
	Protocol string `yaml:"protocol"`
	// ServiceNamespace and ServiceVersion label every span, so traces from
	// two deployments of this platform stay distinguishable in one backend.
	ServiceNamespace string `yaml:"service_namespace"`
	ServiceVersion   string `yaml:"service_version"`
}

// Load reads and validates the configuration at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	var c Config
	if err := yaml.Unmarshal(raw, &c); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	c.applyDefaults()
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config %s: %w", path, err)
	}
	return &c, nil
}

// applyDefaults fills values that have a safe default, so a minimal config
// file still produces a working process.
func (c *Config) applyDefaults() {
	setString(&c.Gateway.Addr, ":8080")
	setDuration(&c.Gateway.ShutdownTimeout, 30*time.Second)

	setInt(&c.Worker.Concurrency, 8)
	setDuration(&c.Worker.ShutdownTimeout, 60*time.Second)

	setInt(&c.MySQL.MaxOpenConns, 20)
	setInt(&c.MySQL.MaxIdleConns, 5)
	setDuration(&c.MySQL.ConnMaxLifetime, time.Hour)

	setString(&c.Redis.Addr, "127.0.0.1:6379")

	setDuration(&c.Scheduler.LeaseTTL, 30*time.Second)
	setDuration(&c.Scheduler.LeaseRenewInterval, 10*time.Second)
	setInt(&c.Scheduler.MaxMessageAttempts, 3)
	setString(&c.Scheduler.QueueKey, "agent:queue:session-hints")

	setInt(&c.Runtime.CacheSize, 64)
	setDuration(&c.Runtime.IdleTTL, 30*time.Minute)

	setString(&c.Secrets.Resolver, "env")
	setString(&c.Telemetry.Protocol, "grpc")
	setString(&c.Telemetry.ServiceNamespace, "trpc-agent-platform")
	setString(&c.Log.Level, "info")
	setString(&c.Log.Format, "text")
}

// Validate rejects configurations that would fail confusingly at runtime.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.MySQL.DSN) == "" {
		return fmt.Errorf("mysql.dsn is required")
	}
	if strings.Contains(c.MySQL.DSN, "USER:PASSWORD") {
		return fmt.Errorf("mysql.dsn still holds the example placeholder; copy configs/config.example.yaml to configs/config.yaml and fill it in")
	}

	// The renew interval must leave room for at least one retry before the
	// lease expires. Equal values would mean a single slow renew loses the
	// lease and lets a second worker into the same session.
	if c.Scheduler.LeaseRenewInterval >= c.Scheduler.LeaseTTL {
		return fmt.Errorf("scheduler.lease_renew_interval (%s) must be shorter than lease_ttl (%s)",
			c.Scheduler.LeaseRenewInterval, c.Scheduler.LeaseTTL)
	}
	if c.Scheduler.MaxMessageAttempts < 1 {
		return fmt.Errorf("scheduler.max_message_attempts must be at least 1")
	}
	if c.Worker.Concurrency < 1 {
		return fmt.Errorf("worker.concurrency must be at least 1")
	}

	// A rule naming a backend that does not exist would silently fall back to
	// the default and route a tenant's data to the wrong store.
	known := make(map[string]bool, len(c.Storage.Backends))
	for _, b := range c.Storage.Backends {
		known[b.Name] = true
	}
	for dt, name := range c.Storage.Defaults {
		if !known[name] {
			return fmt.Errorf("storage.defaults[%s] names unknown backend %q", dt, name)
		}
	}
	for i, r := range c.Storage.Rules {
		if !known[r.Backend] {
			return fmt.Errorf("storage.rules[%d] names unknown backend %q", i, r.Backend)
		}
	}
	return nil
}

// ResolveSecret turns a secret:// reference into its value.
//
// Values are read on demand and never cached in a struct field, so a
// credential cannot be captured by a struct dump or a log of the config.
func (c *Config) ResolveSecret(ref string) (string, error) {
	if ref == "" {
		return "", nil
	}
	if c.Secrets.Resolver != "env" {
		return "", fmt.Errorf("unsupported secret resolver %q", c.Secrets.Resolver)
	}

	envName, ok := c.Secrets.Mapping[ref]
	if !ok {
		return "", fmt.Errorf("secret reference %q has no mapping", ref)
	}
	v, ok := os.LookupEnv(envName)
	if !ok {
		return "", fmt.Errorf("secret reference %q maps to unset environment variable %s", ref, envName)
	}
	return v, nil
}

// ModelBaseURL returns the endpoint configured for a model name.
func (c *Config) ModelBaseURL(name string) (string, bool) {
	for _, m := range c.Models {
		if m.Name == name {
			return m.BaseURL, true
		}
	}
	return "", false
}

func setString(p *string, def string) {
	if strings.TrimSpace(*p) == "" {
		*p = def
	}
}

func setInt(p *int, def int) {
	if *p == 0 {
		*p = def
	}
}

func setDuration(p *time.Duration, def time.Duration) {
	if *p == 0 {
		*p = def
	}
}
