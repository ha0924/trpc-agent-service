package types

import (
	"strings"
	"testing"
)

func TestApplyBody(t *testing.T) {
	cases := []struct {
		name   string
		policy *AuditPolicy
		body   string
		check  func(*testing.T, string)
	}{
		{
			name:   "full keeps the text",
			policy: &AuditPolicy{BodyMode: BodyFull},
			body:   "hello world",
			check: func(t *testing.T, got string) {
				if got != "hello world" {
					t.Errorf("got %q, want the original", got)
				}
			},
		},
		{
			name:   "drop keeps nothing",
			policy: &AuditPolicy{BodyMode: BodyDrop},
			body:   "hello world",
			check: func(t *testing.T, got string) {
				if got != "" {
					t.Errorf("got %q, want empty", got)
				}
			},
		},
		{
			name:   "hash is comparable but not readable",
			policy: &AuditPolicy{BodyMode: BodyHash},
			body:   "hello world",
			check: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "sha256:") {
					t.Errorf("got %q, want a sha256: prefix", got)
				}
				if strings.Contains(got, "hello") {
					t.Error("the original text must not survive hashing")
				}
			},
		},
		{
			name:   "truncate marks the cut",
			policy: &AuditPolicy{BodyMode: BodyTruncate, BodyMaxChars: 5},
			body:   "hello world",
			check: func(t *testing.T, got string) {
				if !strings.HasPrefix(got, "hello") {
					t.Errorf("got %q, want it to start with the prefix", got)
				}
				// Marked rather than silently cut: nobody should read a
				// truncated body as the whole message.
				if !strings.Contains(got, "truncated") {
					t.Errorf("got %q, want a truncation marker", got)
				}
			},
		},
		{
			name:   "truncate leaves short text alone",
			policy: &AuditPolicy{BodyMode: BodyTruncate, BodyMaxChars: 100},
			body:   "short",
			check: func(t *testing.T, got string) {
				if got != "short" {
					t.Errorf("got %q, want the original", got)
				}
			},
		},
		{
			name:   "unknown mode keeps nothing",
			policy: &AuditPolicy{BodyMode: "invented"},
			body:   "hello world",
			check: func(t *testing.T, got string) {
				// An unrecognised value in the database must not widen
				// exposure — fail toward the strictest reading.
				if got != "" {
					t.Errorf("got %q, want empty for an unknown mode", got)
				}
			},
		},
		{
			name:   "nil policy falls back to the safe default",
			policy: nil,
			body:   strings.Repeat("x", 1000),
			check: func(t *testing.T, got string) {
				// Not the original: an unresolved policy must not become a leak.
				if got == strings.Repeat("x", 1000) {
					t.Error("a nil policy must not retain the full body")
				}
				if !strings.Contains(got, "truncated") {
					t.Errorf("got %q, want the default truncation", got)
				}
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			c.check(t, c.policy.ApplyBody(c.body))
		})
	}
}

func TestApplyBodyDoesNotSplitMultibyteRunes(t *testing.T) {
	p := &AuditPolicy{BodyMode: BodyTruncate, BodyMaxChars: 3}

	// Counted in runes, not bytes. Cutting at byte 3 of this string would
	// leave half a character and produce invalid UTF-8 in the audit log.
	got := p.ApplyBody("你好世界测试")
	if !strings.HasPrefix(got, "你好世") {
		t.Fatalf("got %q, want the first three runes intact", got)
	}
}

func TestHashIsStableAndDistinct(t *testing.T) {
	p := &AuditPolicy{BodyMode: BodyHash}

	// Stable, so a replayed message can be recognised — that is the one
	// capability a privacy-sensitive tenant keeps by choosing hash over drop.
	if a, b := p.ApplyBody("same"), p.ApplyBody("same"); a != b {
		t.Errorf("hash is not stable: %q vs %q", a, b)
	}
	if a, b := p.ApplyBody("one"), p.ApplyBody("two"); a == b {
		t.Error("different bodies must hash differently")
	}
}

func TestDefaultAuditPolicyIsNotPermissive(t *testing.T) {
	p := DefaultAuditPolicy("tenant-x")

	// The default applies to every tenant that has no row, including ones
	// created before this feature existed. If it were BodyFull, shipping the
	// feature would start logging their full message bodies.
	if p.BodyMode == BodyFull {
		t.Error("the default must not retain full bodies")
	}
	if p.RedactLevel == RedactNone {
		t.Error("the default must not disable redaction")
	}
	if err := p.Valid(); err != nil {
		t.Errorf("the default must itself be valid: %v", err)
	}
}

func TestRetainsBody(t *testing.T) {
	cases := map[BodyMode]bool{
		BodyFull:     true,
		BodyTruncate: true,
		BodyHash:     true,
		BodyDrop:     false,
	}
	for mode, want := range cases {
		t.Run(string(mode), func(t *testing.T) {
			p := &AuditPolicy{BodyMode: mode}
			if got := p.RetainsBody(); got != want {
				t.Errorf("RetainsBody() = %v, want %v", got, want)
			}
		})
	}
	// A nil policy defaults to truncation, which retains a prefix.
	var p *AuditPolicy
	if !p.RetainsBody() {
		t.Error("nil policy should report retaining a body")
	}
}
