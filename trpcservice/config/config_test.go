package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write temp config: %v", err)
	}
	return path
}

const minimal = `
mysql:
  dsn: "u:p@tcp(127.0.0.1:3306)/db"
`

func TestLoadAppliesDefaults(t *testing.T) {
	c, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if c.Gateway.Addr != ":8080" {
		t.Errorf("Gateway.Addr = %q", c.Gateway.Addr)
	}
	if c.Scheduler.LeaseTTL != 30*time.Second {
		t.Errorf("LeaseTTL = %v", c.Scheduler.LeaseTTL)
	}
	if c.Scheduler.QueueKey == "" {
		t.Error("QueueKey should have a default")
	}
	if c.Worker.Concurrency != 8 {
		t.Errorf("Concurrency = %d", c.Worker.Concurrency)
	}
}

func TestRenewIntervalMustBeShorterThanTTL(t *testing.T) {
	// Equal values mean one slow renew loses the lease and admits a second
	// worker into the same session, which is exactly the ordering failure the
	// lease exists to prevent.
	body := minimal + `
scheduler:
  lease_ttl: 10s
  lease_renew_interval: 10s
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Fatal("want error when renew interval equals TTL")
	}
}

func TestRejectsExamplePlaceholder(t *testing.T) {
	body := `
mysql:
  dsn: "USER:PASSWORD@tcp(127.0.0.1:3306)/db"
`
	_, err := Load(writeConfig(t, body))
	if err == nil {
		t.Fatal("want error for unedited example placeholder")
	}
}

func TestRejectsMissingDSN(t *testing.T) {
	if _, err := Load(writeConfig(t, "gateway:\n  addr: \":9090\"\n")); err == nil {
		t.Fatal("want error when mysql.dsn is absent")
	}
}

func TestStorageRuleMustNameKnownBackend(t *testing.T) {
	// A rule pointing at a non-existent backend would silently fall through
	// to the default and put a tenant's data in the wrong store.
	body := minimal + `
storage:
  backends:
    - name: redis-main
      kind: redis
  rules:
    - tenant_id: t1
      data_type: session
      backend: does-not-exist
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Fatal("want error for unknown backend in rule")
	}
}

func TestStorageDefaultMustNameKnownBackend(t *testing.T) {
	body := minimal + `
storage:
  backends:
    - name: redis-main
      kind: redis
  defaults:
    session: typo-main
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Fatal("want error for unknown backend in defaults")
	}
}

func TestResolveSecretFromEnv(t *testing.T) {
	body := minimal + `
secrets:
  resolver: env
  mapping:
    "secret://prod/t1/model/key": TEST_MODEL_KEY
`
	c, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	t.Setenv("TEST_MODEL_KEY", "sk-secret")
	got, err := c.ResolveSecret("secret://prod/t1/model/key")
	if err != nil {
		t.Fatalf("ResolveSecret: %v", err)
	}
	if got != "sk-secret" {
		t.Errorf("got %q", got)
	}
}

func TestResolveSecretFailsLoudly(t *testing.T) {
	c, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// An unmapped reference must be an error, never an empty string: an empty
	// credential would surface later as a confusing auth failure.
	if _, err := c.ResolveSecret("secret://unknown"); err == nil {
		t.Error("want error for unmapped reference")
	}

	// Mapped but unset is equally an error.
	c.Secrets.Mapping = map[string]string{"secret://x": "DEFINITELY_UNSET_VAR_X"}
	if _, err := c.ResolveSecret("secret://x"); err == nil {
		t.Error("want error for unset environment variable")
	}
}

func TestExampleConfigIsValid(t *testing.T) {
	// The shipped example must parse, so a newcomer copying it gets a working
	// file rather than a parse error. It intentionally fails validation on the
	// DSN placeholder, which is asserted separately above.
	raw, err := os.ReadFile("../../configs/config.example.yaml")
	if err != nil {
		t.Skipf("example config not found: %v", err)
	}
	path := writeConfig(t, string(raw))
	_, err = Load(path)
	if err == nil {
		t.Fatal("example should fail validation on its DSN placeholder")
	}
	if got := err.Error(); !contains(got, "placeholder") {
		t.Fatalf("example failed for the wrong reason: %v", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (func() bool {
		for i := 0; i+len(sub) <= len(s); i++ {
			if s[i:i+len(sub)] == sub {
				return true
			}
		}
		return false
	})()
}
