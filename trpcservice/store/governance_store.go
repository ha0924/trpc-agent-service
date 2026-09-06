// 设计依据：docs/治理监控与安全设计.md
//                docs/数据模型设计.md §1.6「审计与治理记录」、§1.7「不落表的两项」

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// WriteAudit persists one audit record synchronously.
//
// Callers that must not be blocked should use AsyncAuditSink instead — but
// anything recording a dangerous tool has to use this, because the record has
// to be durable before the side effect runs.
//
// The tenant's audit policy is applied here rather than at each call site.
// That is deliberate: a rule enforced by convention gets forgotten at the one
// call site that matters, and this function is the single choke point every
// record passes through.
func (s *Store) WriteAudit(ctx context.Context, r *types.AuditRecord) error {
	s.applyAuditPolicy(ctx, r)

	detail, err := encodeJSON(r.Detail)
	if err != nil {
		return fmt.Errorf("encode audit detail: %w", err)
	}

	const q = `
INSERT INTO audit_logs
  (tenant_id, agent_app_id, agent_name, channel, user_id, session_id,
   request_id, trace_id, event_type, tool_name, decision, reason,
   latency_ms, error_type, cost_usd, detail)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, q,
		r.TenantID, nullString(r.AgentAppID), nullString(r.AgentName),
		nullString(r.Channel), nullString(r.UserID), nullString(r.SessionID),
		r.RequestID, nullString(r.TraceID), r.EventType, nullString(r.ToolName),
		r.Decision, nullString(r.Reason),
		nullInt64(r.LatencyMS), nullString(r.ErrorType), nullFloat(r.CostUSD), detail,
	)
	if err != nil {
		return fmt.Errorf("insert audit record: %w", err)
	}
	return nil
}

// WriteUsage persists one model call's token and cost detail.
func (s *Store) WriteUsage(ctx context.Context, u *types.UsageRecord) error {
	const q = `
INSERT INTO usage_records
  (tenant_id, agent_app_id, agent_version, session_id, request_id, trace_id,
   model_name, prompt_tokens, completion_tokens, total_tokens, cost_usd, latency_ms)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, q,
		u.TenantID, u.AgentAppID, nullString(u.AgentVersion), nullString(u.SessionID),
		u.RequestID, nullString(u.TraceID), u.ModelName,
		u.PromptTokens, u.CompletionTokens, u.TotalTokens, u.CostUSD,
		nullInt64(u.LatencyMS),
	)
	if err != nil {
		return fmt.Errorf("insert usage record: %w", err)
	}
	return nil
}

// CreateToolApproval records a pending confirmation.
//
// It returns only after the row is committed. That ordering is the point: a
// dangerous tool must not run until its intent is durable, so a crash between
// intent and effect still leaves evidence of what was attempted.
func (s *Store) CreateToolApproval(ctx context.Context, a *types.ToolApproval) error {
	args, err := encodeJSON(a.ToolArgs)
	if err != nil {
		return fmt.Errorf("encode approval args: %w", err)
	}

	const q = `
INSERT INTO tool_approvals
  (approval_id, tenant_id, agent_app_id, session_id, request_id, trace_id,
   tool_name, tool_args, requested_by, state, expires_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, q,
		a.ApprovalID, a.TenantID, a.AgentAppID, a.SessionID, a.RequestID,
		nullString(a.TraceID), a.ToolName, args, nullString(a.RequestedBy),
		a.State, nullTime(a.ExpiresAt),
	)
	if err != nil {
		return fmt.Errorf("insert tool approval: %w", err)
	}
	return nil
}

// ResolveToolApproval records a decision on a pending confirmation.
func (s *Store) ResolveToolApproval(ctx context.Context, approvalID string, state types.ApprovalState, decidedBy, reason string) error {
	const q = `
UPDATE tool_approvals SET state = ?, decided_by = ?, reason = ?
 WHERE approval_id = ? AND state = 'pending'`

	res, err := s.db.ExecContext(ctx, q, state, nullString(decidedBy), nullString(reason), approvalID)
	if err != nil {
		return fmt.Errorf("resolve approval %s: %w", approvalID, err)
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		// Either it does not exist or it was already decided. Both must fail
		// loudly: silently accepting a second decision would let a rejected
		// call be re-approved.
		return fmt.Errorf("approval %s: %w", approvalID, ErrNotFound)
	}
	return nil
}

// ChannelUserByExternalID resolves an external IM identity.
func (s *Store) ChannelUserByExternalID(ctx context.Context, bindingID, externalUserID string) (*types.ChannelUser, error) {
	const q = `
SELECT tenant_id, channel_binding_id, external_user_id, internal_user_id,
       COALESCE(display_name, ''), attributes, status
  FROM channel_users
 WHERE channel_binding_id = ? AND external_user_id = ?`

	var (
		u        types.ChannelUser
		attrsRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, bindingID, externalUserID).Scan(
		&u.TenantID, &u.ChannelBindingID, &u.ExternalUserID, &u.InternalUserID,
		&u.DisplayName, &attrsRaw, &u.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("channel user %s/%s: %w", bindingID, externalUserID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query channel user %s/%s: %w", bindingID, externalUserID, err)
	}
	if err := decodeJSON(attrsRaw, &u.Attributes); err != nil {
		return nil, fmt.Errorf("decode attributes for %s: %w", externalUserID, err)
	}
	return &u, nil
}

// TenantSettings reads a tenant's policy knobs.
func (s *Store) TenantSettings(ctx context.Context, tenantID string) (*types.TenantSettings, error) {
	const q = `SELECT settings FROM tenants WHERE tenant_id = ?`

	var raw []byte
	err := s.db.QueryRowContext(ctx, q, tenantID).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("tenant %q: %w", tenantID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query tenant settings %q: %w", tenantID, err)
	}

	var out types.TenantSettings
	if err := decodeJSON(raw, &out); err != nil {
		return nil, fmt.Errorf("decode settings for %q: %w", tenantID, err)
	}
	return &out, nil
}

// AsyncAuditSink writes ordinary audit records off the reply path.
//
// Audit writes on the hot path add database latency to every user's reply. The
// buffer absorbs bursts; when it is full, records are dropped and counted
// rather than blocking, because an audit backlog must not become an outage.
// Dropping is visible in the log so the buffer can be resized.
//
// Anything that must not be dropped — dangerous tool intent above all — has to
// go through Store.WriteAudit directly.
type AsyncAuditSink struct {
	store  *Store
	log    *slog.Logger
	buf    chan *types.AuditRecord
	wg     sync.WaitGroup
	closed chan struct{}
	once   sync.Once

	mu      sync.Mutex
	dropped int64
}

var _ types.AuditSink = (*AsyncAuditSink)(nil)

// NewAsyncAuditSink starts the background writer.
func NewAsyncAuditSink(s *Store, logger *slog.Logger, bufSize int) *AsyncAuditSink {
	if logger == nil {
		logger = slog.Default()
	}
	if bufSize <= 0 {
		bufSize = 1024
	}

	sink := &AsyncAuditSink{
		store: s, log: logger,
		buf:    make(chan *types.AuditRecord, bufSize),
		closed: make(chan struct{}),
	}
	sink.wg.Add(1)
	go sink.loop()
	return sink
}

func (a *AsyncAuditSink) loop() {
	defer a.wg.Done()
	for r := range a.buf {
		// A fresh context: the request that produced this record may already
		// have returned, and its cancellation must not discard the audit.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := a.store.WriteAudit(ctx, r); err != nil {
			a.log.Error("audit write failed",
				"tenant_id", r.TenantID, "event_type", r.EventType,
				"request_id", r.RequestID, "error", err.Error())
		}
		cancel()
	}
}

// Write queues a record, dropping it if the buffer is full.
func (a *AsyncAuditSink) Write(_ context.Context, r *types.AuditRecord) error {
	select {
	case <-a.closed:
		return errors.New("store: audit sink closed")
	default:
	}

	select {
	case a.buf <- r:
		return nil
	default:
		a.mu.Lock()
		a.dropped++
		n := a.dropped
		a.mu.Unlock()
		if n%100 == 1 {
			a.log.Warn("audit buffer full, records dropped", "dropped_total", n)
		}
		return nil
	}
}

// Dropped reports how many records were discarded, for metrics.
func (a *AsyncAuditSink) Dropped() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.dropped
}

// Close flushes the buffer and stops the writer.
func (a *AsyncAuditSink) Close() error {
	a.once.Do(func() {
		close(a.closed)
		close(a.buf)
		a.wg.Wait()
	})
	return nil
}

func nullInt64(v int64) any {
	if v == 0 {
		return nil
	}
	return v
}

func nullFloat(v float64) any {
	if v == 0 {
		return nil
	}
	return v
}
