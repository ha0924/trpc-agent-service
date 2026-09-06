// 设计依据：docs/治理监控与安全设计.md §8「密钥与脱敏」
//                docs/多租户与节点部署设计.md §2「租户资源模型」第 7 要素

package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// AuditPolicy is the seventh element of the tenant model: how much of a
// message body may survive into the audit log.
//
// It has to be per tenant because tenants want opposite things. A regulated
// one needs the original text retained as evidence; a privacy-sensitive one
// needs it never written at all. Hardcoding either makes the platform unusable
// for the other, which is why this is configuration rather than a constant.
//
// Redaction of *credentials* is not configurable and is not represented here.
// Secrets are always stripped, at every level including RedactNone — a tenant
// must not be able to opt into leaking API keys. This policy governs only
// user-authored content.
type AuditPolicy struct {
	TenantID string `json:"tenant_id"`

	RedactLevel RedactLevel `json:"redact_level"`
	BodyMode    BodyMode    `json:"body_mode"`

	// BodyMaxChars applies when BodyMode is BodyTruncate. Counted in runes,
	// not bytes: cutting a multi-byte character in half would corrupt the
	// stored text.
	BodyMaxChars int `json:"body_max_chars"`

	RetentionDays int `json:"retention_days"`
}

// RedactLevel controls how aggressively personal data is masked.
type RedactLevel string

const (
	// RedactNone masks nothing beyond credentials. Local debugging only.
	RedactNone RedactLevel = "none"
	// RedactStandard masks credentials and obvious personal identifiers.
	RedactStandard RedactLevel = "standard"
	// RedactStrict additionally drops anything not on a field allowlist.
	RedactStrict RedactLevel = "strict"
)

// BodyMode controls what happens to message text.
type BodyMode string

const (
	// BodyFull keeps the text as written.
	BodyFull BodyMode = "full"
	// BodyTruncate keeps a prefix, enough to recognise a conversation without
	// retaining all of it.
	BodyTruncate BodyMode = "truncate"
	// BodyHash keeps only a digest: two records can be compared for equality
	// and nothing else. This is what lets a privacy-sensitive tenant still
	// detect a replayed message.
	BodyHash BodyMode = "hash"
	// BodyDrop keeps nothing.
	BodyDrop BodyMode = "drop"
)

// DefaultAuditPolicy is applied when a tenant has no row.
//
// Deliberately not the most permissive option. A missing configuration must
// not silently become "retain everything" — a tenant created before this
// feature existed would then start logging full message bodies the moment the
// code shipped. Truncation is the safe default: useful for debugging, bounded
// in exposure.
func DefaultAuditPolicy(tenantID string) *AuditPolicy {
	return &AuditPolicy{
		TenantID:      tenantID,
		RedactLevel:   RedactStandard,
		BodyMode:      BodyTruncate,
		BodyMaxChars:  512,
		RetentionDays: 90,
	}
}

// Valid reports whether the policy is usable, so a bad configuration is
// rejected at write time rather than surfacing per audit record.
func (p *AuditPolicy) Valid() error {
	if p == nil {
		return fmt.Errorf("audit policy: nil")
	}
	switch p.RedactLevel {
	case RedactNone, RedactStandard, RedactStrict:
	default:
		return fmt.Errorf("audit policy: unknown redact_level %q", p.RedactLevel)
	}
	switch p.BodyMode {
	case BodyFull, BodyTruncate, BodyHash, BodyDrop:
	default:
		return fmt.Errorf("audit policy: unknown body_mode %q", p.BodyMode)
	}
	if p.BodyMode == BodyTruncate && p.BodyMaxChars <= 0 {
		return fmt.Errorf("audit policy: body_max_chars must be positive when truncating")
	}
	if p.RetentionDays <= 0 {
		return fmt.Errorf("audit policy: retention_days must be positive")
	}
	return nil
}

// ApplyBody transforms one body according to the policy.
//
// The caller has already scrubbed credentials; this decides how much of what
// remains is kept. Returning the transformed string rather than mutating in
// place keeps the original available to the caller, which still needs it to
// answer the user.
func (p *AuditPolicy) ApplyBody(body string) string {
	if p == nil {
		// No policy resolved. Falling back to the safe default rather than to
		// the original text: an unresolved policy must not become a leak.
		p = DefaultAuditPolicy("")
	}
	if body == "" {
		return ""
	}

	switch p.BodyMode {
	case BodyFull:
		return body

	case BodyDrop:
		return ""

	case BodyHash:
		// Prefixed so a reader can tell a digest from text that merely looks
		// like one, and truncated to 16 bytes — enough to compare, not enough
		// to be mistaken for a full checksum of anything.
		sum := sha256.Sum256([]byte(body))
		return "sha256:" + hex.EncodeToString(sum[:16])

	case BodyTruncate:
		limit := p.BodyMaxChars
		if limit <= 0 {
			limit = 512
		}
		runes := []rune(body)
		if len(runes) <= limit {
			return body
		}
		// Marked rather than silently cut, so nobody reads a truncated body as
		// the whole message.
		return string(runes[:limit]) + "…[truncated]"

	default:
		// Unknown mode: treat as the strictest interpretation. An
		// unrecognised value in the database must not widen exposure.
		return ""
	}
}

// RetainsBody reports whether any of the original text survives, so callers
// can skip assembling a body the policy will discard.
func (p *AuditPolicy) RetainsBody() bool {
	if p == nil {
		return true // default truncates, which retains a prefix
	}
	return p.BodyMode != BodyDrop
}
