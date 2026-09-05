package log

import (
	"strings"
	"testing"
)

func TestScrubRemovesCredentials(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		absent  string
		present string
	}{
		{
			name:    "dsn password",
			in:      "dial mysql root:s3cr3t-pass@tcp(127.0.0.1:3306)/db: timeout",
			absent:  "s3cr3t-pass",
			present: "root:",
		},
		{
			name:   "bearer token",
			in:     "upstream rejected: Authorization Bearer abcdef1234567890",
			absent: "abcdef1234567890",
		},
		{
			name:   "provider key",
			in:     "model call failed with key sk-abcdef1234567890",
			absent: "sk-abcdef1234567890",
		},
		{
			name:   "key value pair",
			in:     "config parse error near api_key=super-secret-value",
			absent: "super-secret-value",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := Scrub(c.in)
			if strings.Contains(got, c.absent) {
				t.Errorf("credential survived scrubbing:\n in: %s\nout: %s", c.in, got)
			}
			if !strings.Contains(got, Redacted) {
				t.Errorf("no redaction marker in output: %s", got)
			}
			// The message has to stay diagnosable, otherwise engineers will
			// route around the logger to debug.
			if c.present != "" && !strings.Contains(got, c.present) {
				t.Errorf("lost diagnostic context %q: %s", c.present, got)
			}
		})
	}
}

func TestScrubLeavesCleanTextAlone(t *testing.T) {
	clean := "session sess-123 acquired lease for tenant tenant-demo"
	if got := Scrub(clean); got != clean {
		t.Errorf("clean text was modified:\n in: %s\nout: %s", clean, got)
	}
}

func TestSecretReferenceIsNotRedacted(t *testing.T) {
	// A secret:// reference names a credential without revealing it, and it
	// is genuinely useful in logs when diagnosing a resolution failure.
	ref := "secret://prod/tenant-demo/model/primary-api-key"
	if got := Scrub(ref); got != ref {
		t.Errorf("secret reference should survive: %s", got)
	}
}

func TestParseLevel(t *testing.T) {
	for in, want := range map[string]string{
		"debug": "DEBUG", "info": "INFO", "WARN": "WARN",
		"error": "ERROR", "nonsense": "INFO", "": "INFO",
	} {
		if got := parseLevel(in).String(); got != want {
			t.Errorf("parseLevel(%q) = %s, want %s", in, got, want)
		}
	}
}
