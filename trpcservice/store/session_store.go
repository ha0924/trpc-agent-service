// 设计依据：docs/IM通道接入设计.md §5「入站流程」、§6「身份与会话映射」
//                docs/数据模型设计.md §5.10-5.12

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// SessionLookup locates or creates a session for one inbound message.
type SessionLookup struct {
	Binding  *types.ChannelBinding
	Scope    types.Scope
	ScopeKey string
	// InternalUserID is empty for group sessions, where the participants are
	// many and the speaker is recorded on each event instead.
	InternalUserID string
}

// FindOrCreateSession returns the session for this conversation, creating it
// on first contact.
//
// Version selection happens here and only here. A new session draws a version
// by deployment weight and freezes it; an existing session keeps whatever it
// was created with, so publishing a new version or rolling back never changes
// the behaviour of a conversation already under way.
//
// The bool reports whether the session was created by this call.
func (s *Store) FindOrCreateSession(ctx context.Context, in SessionLookup) (*types.Session, bool, error) {
	if in.Binding == nil {
		return nil, false, errors.New("store: nil channel binding")
	}

	sess, err := s.sessionByScope(ctx, in.Binding.ChannelBindingID, in.Scope, in.ScopeKey)
	if err == nil {
		return sess, false, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return nil, false, err
	}

	deployment, err := s.Deployment(ctx, in.Binding.TenantID, in.Binding.AgentAppID, in.Binding.Env)
	if err != nil {
		return nil, false, err
	}
	version, err := deployment.PickVersion(rand.Intn(max(deployment.TotalWeight(), 1)))
	if err != nil {
		return nil, false, err
	}

	created := &types.Session{
		SessionID:        "sess-" + uuid.NewString(),
		TenantID:         in.Binding.TenantID,
		AgentAppID:       in.Binding.AgentAppID,
		AgentVersion:     version,
		Channel:          in.Binding.Channel,
		ChannelBindingID: in.Binding.ChannelBindingID,
		Scope:            in.Scope,
		ScopeKey:         in.ScopeKey,
		InternalUserID:   in.InternalUserID,
		Status:           types.StatusActive,
	}

	const q = `
INSERT INTO sessions
  (session_id, tenant_id, agent_app_id, agent_version, channel,
   channel_binding_id, scope, scope_key, internal_user_id,
   last_sequence, status, last_active_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, NOW(3))`

	_, err = s.db.ExecContext(ctx, q,
		created.SessionID, created.TenantID, created.AgentAppID, created.AgentVersion,
		created.Channel, created.ChannelBindingID, created.Scope, created.ScopeKey,
		nullString(created.InternalUserID), created.Status,
	)
	if isDuplicate(err) {
		// Another request created the same conversation between our SELECT
		// and INSERT. uk_scope caught it; re-read so both requests agree on
		// one session and, critically, on one frozen version.
		sess, readErr := s.sessionByScope(ctx, in.Binding.ChannelBindingID, in.Scope, in.ScopeKey)
		if readErr != nil {
			return nil, false, fmt.Errorf("reread session after duplicate: %w", readErr)
		}
		return sess, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("insert session for %s/%s: %w",
			in.Binding.ChannelBindingID, in.ScopeKey, err)
	}
	return created, true, nil
}

func (s *Store) sessionByScope(ctx context.Context, bindingID string, scope types.Scope, scopeKey string) (*types.Session, error) {
	const q = `
SELECT session_id, tenant_id, agent_app_id, agent_version, channel,
       channel_binding_id, scope, scope_key, COALESCE(internal_user_id, ''),
       last_sequence, status, last_active_at
  FROM sessions
 WHERE channel_binding_id = ? AND scope = ? AND scope_key = ?`

	var sess types.Session
	err := s.db.QueryRowContext(ctx, q, bindingID, scope, scopeKey).Scan(
		&sess.SessionID, &sess.TenantID, &sess.AgentAppID, &sess.AgentVersion,
		&sess.Channel, &sess.ChannelBindingID, &sess.Scope, &sess.ScopeKey,
		&sess.InternalUserID, &sess.LastSequence, &sess.Status, &sess.LastActiveAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("session for %s/%s/%s: %w", bindingID, scope, scopeKey, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query session %s/%s/%s: %w", bindingID, scope, scopeKey, err)
	}
	return &sess, nil
}

// SessionByID loads a session by its business identifier.
func (s *Store) SessionByID(ctx context.Context, tenantID, sessionID string) (*types.Session, error) {
	const q = `
SELECT session_id, tenant_id, agent_app_id, agent_version, channel,
       channel_binding_id, scope, scope_key, COALESCE(internal_user_id, ''),
       last_sequence, status, last_active_at
  FROM sessions
 WHERE tenant_id = ? AND session_id = ?`

	var sess types.Session
	err := s.db.QueryRowContext(ctx, q, tenantID, sessionID).Scan(
		&sess.SessionID, &sess.TenantID, &sess.AgentAppID, &sess.AgentVersion,
		&sess.Channel, &sess.ChannelBindingID, &sess.Scope, &sess.ScopeKey,
		&sess.InternalUserID, &sess.LastSequence, &sess.Status, &sess.LastActiveAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("session %q: %w", sessionID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query session %q: %w", sessionID, err)
	}
	return &sess, nil
}

// InsertInboundEvent writes the idempotency record.
//
// It returns false, nil when the record already exists. That is the expected
// outcome of a platform redelivering a message, not an error: the caller
// simply ACKs again without re-running the agent.
//
// This write happens before the ACK and before queueing. Writing it first
// means an in-flight request always has a durable record, so a hint lost by
// the queue can be recovered by sweeping rows stuck in processing.
func (s *Store) InsertInboundEvent(ctx context.Context, ev *types.InboundEvent) (bool, error) {
	payload, err := encodeJSON(ev.Payload)
	if err != nil {
		return false, fmt.Errorf("encode inbound payload: %w", err)
	}

	const q = `
INSERT INTO inbound_events
  (tenant_id, channel_binding_id, external_event_id, request_id, trace_id,
   session_id, payload, state, attempts)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`

	_, err = s.db.ExecContext(ctx, q,
		ev.TenantID, ev.ChannelBindingID, ev.ExternalEventID, ev.RequestID,
		nullString(ev.TraceID), nullString(ev.SessionID), payload, ev.State,
	)
	if isDuplicate(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("insert inbound event %s/%s: %w",
			ev.ChannelBindingID, ev.ExternalEventID, err)
	}
	return true, nil
}

// UpdateInboundState advances the two-phase lifecycle of an inbound event.
//
// Execution and delivery are distinct states because a delivery failure must
// retry only the delivery. Collapsing them would rerun the agent and repeat
// every tool call, which is unacceptable for tools with side effects.
func (s *Store) UpdateInboundState(ctx context.Context, bindingID, externalEventID string, state types.InboundState, lastErr string) error {
	const q = `
UPDATE inbound_events
   SET state = ?, last_error = ?, attempts = attempts + 1
 WHERE channel_binding_id = ? AND external_event_id = ?`

	res, err := s.db.ExecContext(ctx, q, state, nullString(lastErr), bindingID, externalEventID)
	if err != nil {
		return fmt.Errorf("update inbound state %s/%s: %w", bindingID, externalEventID, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for %s/%s: %w", bindingID, externalEventID, err)
	}
	if n == 0 {
		return fmt.Errorf("inbound event %s/%s: %w", bindingID, externalEventID, ErrNotFound)
	}
	return nil
}

// SetInboundSession backfills the session id once the conversation is known.
func (s *Store) SetInboundSession(ctx context.Context, bindingID, externalEventID, sessionID string) error {
	const q = `
UPDATE inbound_events SET session_id = ?
 WHERE channel_binding_id = ? AND external_event_id = ?`

	if _, err := s.db.ExecContext(ctx, q, sessionID, bindingID, externalEventID); err != nil {
		return fmt.Errorf("set inbound session %s/%s: %w", bindingID, externalEventID, err)
	}
	return nil
}

// AppendSessionEvent writes one event and advances the session's sequence.
//
// Both statements run in one transaction with the session row locked, so two
// workers cannot allocate the same sequence. The uk_seq unique key is the
// second line of defence: if a lease ever lapses and two workers do write
// concurrently, the duplicate sequence fails the insert instead of silently
// producing an out-of-order conversation.
func (s *Store) AppendSessionEvent(ctx context.Context, ev *types.SessionEvent) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("begin tx for session event: %w", err)
	}
	defer tx.Rollback()

	var last int64
	const lockQ = `SELECT last_sequence FROM sessions WHERE tenant_id = ? AND session_id = ? FOR UPDATE`
	err = tx.QueryRowContext(ctx, lockQ, ev.TenantID, ev.SessionID).Scan(&last)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("session %q: %w", ev.SessionID, ErrNotFound)
	}
	if err != nil {
		return 0, fmt.Errorf("lock session %q: %w", ev.SessionID, err)
	}
	next := last + 1

	content, err := encodeJSON(ev.Content)
	if err != nil {
		return 0, fmt.Errorf("encode event content: %w", err)
	}

	const insertQ = `
INSERT INTO session_events
  (tenant_id, session_id, sequence, event_type, role, content,
   request_id, trace_id, agent_version)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`

	_, err = tx.ExecContext(ctx, insertQ,
		ev.TenantID, ev.SessionID, next, ev.EventType, nullString(ev.Role), content,
		ev.RequestID, nullString(ev.TraceID), nullString(ev.AgentVersion),
	)
	if isDuplicate(err) {
		return 0, fmt.Errorf("session %s sequence %d already written: %w", ev.SessionID, next, ErrDuplicate)
	}
	if err != nil {
		return 0, fmt.Errorf("insert session event for %s: %w", ev.SessionID, err)
	}

	const bumpQ = `UPDATE sessions SET last_sequence = ?, last_active_at = NOW(3) WHERE tenant_id = ? AND session_id = ?`
	if _, err := tx.ExecContext(ctx, bumpQ, next, ev.TenantID, ev.SessionID); err != nil {
		return 0, fmt.Errorf("bump sequence for %s: %w", ev.SessionID, err)
	}

	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit session event for %s: %w", ev.SessionID, err)
	}
	return next, nil
}
