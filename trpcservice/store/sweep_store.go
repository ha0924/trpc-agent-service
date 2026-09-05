// 设计依据：docs/风险清单.md 其他已知约束「消息队列丢消息」
//                docs/数据模型设计.md §5.12 inbound_events「idx_state 用于扫描」

package store

import (
	"context"
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
