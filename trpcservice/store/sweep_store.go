// 设计依据：docs/风险清单.md 其他已知约束「消息队列丢消息」
//                docs/数据模型设计.md §5.12 inbound_events「idx_state 用于扫描」

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// StaleInbound is an in-flight request that has not reached a terminal state.
type StaleInbound struct {
	TenantID         string
	ChannelBindingID string
	ExternalEventID  string
	RequestID        string
	TraceID          string
	SessionID        string
	Payload          *types.InboundMessage
	Attempts         int
	UpdatedAt        time.Time
}

// FindStaleInbound returns requests stuck in processing for longer than age.
//
// This is what makes the queue allowed to be lossy. A hint that never reached
// a Worker — dropped by Redis, lost in a restart — leaves its inbound_events
// row in processing forever, and nothing else would ever notice. Writing that
// row before the ACK is only useful if something reads it back.
//
// It is also what makes the retry budget real. A message that fails is left in
// processing rather than retried inline; without a sweep to requeue it, the
// attempt counter never advances and max_message_attempts never triggers.
//
// The age threshold has to exceed the longest plausible agent run, or the
// sweep would requeue work that is still in progress and two Workers would
// process the same message.
func (s *Store) FindStaleInbound(ctx context.Context, age time.Duration, limit int) ([]StaleInbound, error) {
	const q = `
SELECT tenant_id, channel_binding_id, external_event_id, request_id,
       COALESCE(trace_id, ''), COALESCE(session_id, ''), payload, attempts, updated_at
  FROM inbound_events
 WHERE state = ? AND updated_at < ?
 ORDER BY updated_at
 LIMIT ?`

	cutoff := time.Now().Add(-age)
	rows, err := s.db.QueryContext(ctx, q, types.StateProcessing, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("query stale inbound events: %w", err)
	}
	defer rows.Close()

	var out []StaleInbound
	for rows.Next() {
		var (
			si  StaleInbound
			raw []byte
		)
		if err := rows.Scan(&si.TenantID, &si.ChannelBindingID, &si.ExternalEventID,
			&si.RequestID, &si.TraceID, &si.SessionID, &raw, &si.Attempts, &si.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan stale inbound event: %w", err)
		}
		if err := decodeJSON(raw, &si.Payload); err != nil {
			// A row whose payload cannot be decoded can never be replayed.
			// Skipped rather than failing the whole sweep, and it stays
			// visible in the table for someone to look at.
			continue
		}
		if si.Payload == nil || si.SessionID == "" {
			continue
		}
		out = append(out, si)
	}
	return out, rows.Err()
}

// TouchInbound bumps updated_at so a requeued row is not picked up again on
// the next sweep before a Worker has had time to take it.
//
// Without this, a sweep running every minute against a row that takes two
// minutes to process would requeue it repeatedly and several Workers would
// contend for the same message. The lease would keep them from corrupting the
// conversation, but the work would still be done more than once.
func (s *Store) TouchInbound(ctx context.Context, bindingID, externalEventID string) error {
	const q = `
UPDATE inbound_events SET updated_at = NOW(3)
 WHERE channel_binding_id = ? AND external_event_id = ?`

	if _, err := s.db.ExecContext(ctx, q, bindingID, externalEventID); err != nil {
		return fmt.Errorf("touch inbound event %s/%s: %w", bindingID, externalEventID, err)
	}
	return nil
}

// FindFailedDeliveries returns requests whose agent finished but whose reply
// never reached the user.
//
// This is a distinct query from FindStaleInbound because the two need opposite
// treatment. A stranded request has to be re-executed; a failed delivery must
// **not** be — its tools already ran and their side effects already happened.
// Re-running it would place a second order, send a second notification, charge
// a second time.
//
// The reply itself is recovered from the session's last agent message rather
// than being stored a second time on this row: duplicating it would give two
// copies that can disagree.
func (s *Store) FindFailedDeliveries(ctx context.Context, age time.Duration, maxAttempts, limit int) ([]StaleInbound, error) {
	const q = `
SELECT tenant_id, channel_binding_id, external_event_id, request_id,
       COALESCE(trace_id, ''), COALESCE(session_id, ''), payload, attempts, updated_at
  FROM inbound_events
 WHERE state = ? AND updated_at < ? AND attempts < ?
 ORDER BY updated_at
 LIMIT ?`

	cutoff := time.Now().Add(-age)
	rows, err := s.db.QueryContext(ctx, q, types.StateDeliveryFailed, cutoff, maxAttempts, limit)
	if err != nil {
		return nil, fmt.Errorf("query failed deliveries: %w", err)
	}
	defer rows.Close()

	var out []StaleInbound
	for rows.Next() {
		var (
			si  StaleInbound
			raw []byte
		)
		if err := rows.Scan(&si.TenantID, &si.ChannelBindingID, &si.ExternalEventID,
			&si.RequestID, &si.TraceID, &si.SessionID, &raw, &si.Attempts, &si.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan failed delivery: %w", err)
		}
		if err := decodeJSON(raw, &si.Payload); err != nil {
			continue
		}
		if si.Payload == nil || si.SessionID == "" {
			continue
		}
		out = append(out, si)
	}
	return out, rows.Err()
}

// LastAgentReply returns the most recent agent message in a session.
//
// Used to redeliver a reply that was produced but not delivered. Read back
// from session_events rather than kept on the inbound row, so there is one
// copy of the answer and no chance of the two diverging.
func (s *Store) LastAgentReply(ctx context.Context, tenantID, sessionID, requestID string) (string, error) {
	const q = `
SELECT JSON_UNQUOTE(JSON_EXTRACT(content, '$.text'))
  FROM session_events
 WHERE tenant_id = ? AND session_id = ? AND request_id = ? AND event_type = ?
 ORDER BY sequence DESC
 LIMIT 1`

	var text sql.NullString
	err := s.db.QueryRowContext(ctx, q, tenantID, sessionID, requestID,
		types.EventTypeAgentMessage).Scan(&text)
	if errors.Is(err, sql.ErrNoRows) {
		return "", fmt.Errorf("agent reply for request %s: %w", requestID, ErrNotFound)
	}
	if err != nil {
		return "", fmt.Errorf("query agent reply for %s: %w", requestID, err)
	}
	if !text.Valid || text.String == "" {
		return "", fmt.Errorf("agent reply for request %s is empty: %w", requestID, ErrNotFound)
	}
	return text.String, nil
}
