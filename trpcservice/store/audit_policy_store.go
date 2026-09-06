// 设计依据：docs/治理监控与安全设计.md §8「密钥与脱敏」
//                docs/多租户与节点部署设计.md §2「租户资源模型」第 7 要素

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Audit policies are read on every audit write, so they are cached.
//
// Without a cache each record would cost an extra query, and audit writes sit
// on the request path. The TTL is short because a policy change is an
// operational action whose effect should be visible quickly — an operator who
// tightens redaction after a leak must not wait out a long TTL.
const auditPolicyTTL = 30 * time.Second

// auditPolicyCache holds resolved policies per tenant.
type auditPolicyCache struct {
	mu      sync.RWMutex
	entries map[string]auditPolicyEntry
}

type auditPolicyEntry struct {
	policy  *types.AuditPolicy
	expires time.Time
}

// AuditPolicy returns a tenant's policy, or the default when none is set.
//
// A missing row is not an error. Tenants created before this table existed
// have none, and they must keep working — on the safe default rather than on
// "retain everything", so shipping this feature cannot start logging full
// message bodies for an existing tenant.
func (s *Store) AuditPolicy(ctx context.Context, tenantID string) (*types.AuditPolicy, error) {
	const q = `
SELECT tenant_id, redact_level, body_mode, body_max_chars, retention_days
  FROM audit_policies WHERE tenant_id = ?`

	var p types.AuditPolicy
	err := s.db.QueryRowContext(ctx, q, tenantID).Scan(
		&p.TenantID, &p.RedactLevel, &p.BodyMode, &p.BodyMaxChars, &p.RetentionDays)
	if errors.Is(err, sql.ErrNoRows) {
		return types.DefaultAuditPolicy(tenantID), nil
	}
	if err != nil {
		return nil, fmt.Errorf("query audit policy for %s: %w", tenantID, err)
	}
	return &p, nil
}

// UpsertAuditPolicy sets a tenant's policy.
//
// Validated before the write so a bad value is rejected once here rather than
// degrading every subsequent audit record.
func (s *Store) UpsertAuditPolicy(ctx context.Context, p *types.AuditPolicy) error {
	if err := p.Valid(); err != nil {
		return err
	}
	if err := s.requireTenant(ctx, p.TenantID); err != nil {
		return err
	}

	const q = `
INSERT INTO audit_policies
  (tenant_id, redact_level, body_mode, body_max_chars, retention_days)
VALUES (?, ?, ?, ?, ?) AS new
ON DUPLICATE KEY UPDATE
  redact_level   = new.redact_level,
  body_mode      = new.body_mode,
  body_max_chars = new.body_max_chars,
  retention_days = new.retention_days`

	if _, err := s.db.ExecContext(ctx, q,
		p.TenantID, p.RedactLevel, p.BodyMode, p.BodyMaxChars, p.RetentionDays); err != nil {
		return fmt.Errorf("upsert audit policy for %s: %w", p.TenantID, err)
	}

	// Dropped rather than replaced: the write may have been a partial update
	// and re-reading is cheap, whereas caching a value assembled here risks
	// diverging from what the database actually holds.
	s.invalidateAuditPolicy(p.TenantID)
	return nil
}

// cachedAuditPolicy resolves a policy through the cache.
func (s *Store) cachedAuditPolicy(ctx context.Context, tenantID string) *types.AuditPolicy {
	if s.policies == nil {
		// Store built without a cache (NewWithDB in tests). Read through.
		p, err := s.AuditPolicy(ctx, tenantID)
		if err != nil {
			return types.DefaultAuditPolicy(tenantID)
		}
		return p
	}

	s.policies.mu.RLock()
	e, ok := s.policies.entries[tenantID]
	s.policies.mu.RUnlock()
	if ok && time.Now().Before(e.expires) {
		return e.policy
	}

	p, err := s.AuditPolicy(ctx, tenantID)
	if err != nil {
		// A failed lookup must not drop the audit record, and must not widen
		// exposure either. The safe default does both.
		return types.DefaultAuditPolicy(tenantID)
	}

	s.policies.mu.Lock()
	s.policies.entries[tenantID] = auditPolicyEntry{
		policy:  p,
		expires: time.Now().Add(auditPolicyTTL),
	}
	s.policies.mu.Unlock()
	return p
}

func (s *Store) invalidateAuditPolicy(tenantID string) {
	if s.policies == nil {
		return
	}
	s.policies.mu.Lock()
	delete(s.policies.entries, tenantID)
	s.policies.mu.Unlock()
}

// applyAuditPolicy trims a record according to its tenant's policy.
//
// Only the fields carrying user-authored text are touched. Decisions, tool
// names, latencies and error types are the audit trail itself: redacting them
// would leave a row that records that something happened without recording
// what, which defeats the purpose of having an audit log.
//
// Credentials are already gone by this point — the Redactor runs earlier and
// is not configurable. A tenant must not be able to opt into logging API keys,
// which is why credential scrubbing is not part of this policy.
func (s *Store) applyAuditPolicy(ctx context.Context, r *types.AuditRecord) {
	if r == nil || r.TenantID == "" {
		return
	}
	p := s.cachedAuditPolicy(ctx, r.TenantID)
	if p == nil || p.BodyMode == types.BodyFull {
		return // nothing to do; the common case costs one comparison
	}

	// Reason is written by guardrails and can quote user input.
	r.Reason = p.ApplyBody(r.Reason)

	if len(r.Detail) == 0 {
		return
	}
	if !p.RetainsBody() {
		// The policy keeps no body at all. Structural keys are preserved so a
		// reader can still tell what kind of record this was.
		for _, k := range bodyDetailKeys {
			delete(r.Detail, k)
		}
		return
	}
	for _, k := range bodyDetailKeys {
		if v, ok := r.Detail[k].(string); ok {
			r.Detail[k] = p.ApplyBody(v)
		}
	}
}

// bodyDetailKeys are the Detail keys known to hold user-authored text.
//
// An allowlist rather than "every string value": Detail also carries ids,
// approval tokens and expiry timestamps, and hashing those would destroy the
// ability to correlate records without protecting anything.
var bodyDetailKeys = []string{"text", "arguments", "input", "output", "prompt", "content"}
