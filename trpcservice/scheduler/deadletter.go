// 设计依据：docs/风险清单.md #5「毒消息阻塞 Session 信箱」
//                docs/dev/迭代计划.md 二期「补充死信处理」

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Dead letters and attempt counters live under their own namespaces so they
// can be inspected and drained without touching live mailboxes.
const (
	prefixDeadLetter = "agent:deadletter:"
	prefixAttempts   = "agent:attempts:"
)

// DeadLetter is a message that exhausted its retries, kept with the reason it
// failed so it can be diagnosed rather than merely counted.
type DeadLetter struct {
	Message   *types.InboundMessage `json:"message"`
	Attempts  int                   `json:"attempts"`
	LastError string                `json:"last_error"`
	FailedAt  time.Time             `json:"failed_at"`
	WorkerID  string                `json:"worker_id"`
}

// attemptKey counts how many times one message has been tried.
//
// Keyed by the platform's request id rather than by session: the bound is per
// message, because one poison message must not consume the retry budget of
// every later message in the same conversation.
func attemptKey(requestID string) string { return prefixAttempts + requestID }

// deadLetterKey is the per-session dead letter list.
//
// Per session rather than one global list, so draining or replaying one
// tenant's failures cannot disturb another's, and so a session's failures stay
// next to the session when diagnosing.
func deadLetterKey(sessionID string) string { return prefixDeadLetter + sessionID }

// RecordAttempt increments a message's attempt counter and returns the new
// count.
//
// The counter carries a TTL so a message that eventually succeeds does not
// leave a key behind forever. The TTL is generous relative to a retry cycle:
// expiring mid-cycle would silently reset the budget and turn a bounded retry
// into an unbounded one.
func (r *Redis) RecordAttempt(ctx context.Context, requestID string) (int, error) {
	key := attemptKey(requestID)

	n, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return 0, fmt.Errorf("increment attempts for %s: %w", requestID, err)
	}
	if n == 1 {
		if err := r.client.Expire(ctx, key, 24*time.Hour).Err(); err != nil {
			return int(n), fmt.Errorf("set attempts ttl for %s: %w", requestID, err)
		}
	}
	return int(n), nil
}

// ClearAttempts drops a message's counter after it succeeds.
func (r *Redis) ClearAttempts(ctx context.Context, requestID string) error {
	if err := r.client.Del(ctx, attemptKey(requestID)).Err(); err != nil {
		return fmt.Errorf("clear attempts for %s: %w", requestID, err)
	}
	return nil
}

// PushDeadLetter moves a message out of the retry path.
//
// This is what keeps one bad message from blocking a conversation. The mailbox
// drains past it and later messages get served; the failure is preserved for
// inspection instead of being retried forever or silently dropped.
func (r *Redis) PushDeadLetter(ctx context.Context, sessionID string, dl *DeadLetter) error {
	payload, err := json.Marshal(dl)
	if err != nil {
		return fmt.Errorf("marshal dead letter: %w", err)
	}

	key := deadLetterKey(sessionID)
	if err := r.client.LPush(ctx, key, payload).Err(); err != nil {
		return fmt.Errorf("push dead letter for %s: %w", sessionID, err)
	}
	// Bounded retention: a session that fails repeatedly must not grow this
	// list without limit. The most recent failures are the diagnostic ones.
	if err := r.client.LTrim(ctx, key, 0, 99).Err(); err != nil {
		return fmt.Errorf("trim dead letters for %s: %w", sessionID, err)
	}
	if err := r.client.Expire(ctx, key, 7*24*time.Hour).Err(); err != nil {
		return fmt.Errorf("set dead letter ttl for %s: %w", sessionID, err)
	}
	return nil
}

// ListDeadLetters returns a session's recorded failures, newest first.
func (r *Redis) ListDeadLetters(ctx context.Context, sessionID string, limit int64) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = 20
	}

	raw, err := r.client.LRange(ctx, deadLetterKey(sessionID), 0, limit-1).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, nil
		}
		return nil, fmt.Errorf("list dead letters for %s: %w", sessionID, err)
	}

	out := make([]DeadLetter, 0, len(raw))
	for _, item := range raw {
		var dl DeadLetter
		if err := json.Unmarshal([]byte(item), &dl); err != nil {
			// A single unparseable entry must not hide the rest.
			continue
		}
		out = append(out, dl)
	}
	return out, nil
}

// DeadLetterCount reports how many failures a session has accumulated.
func (r *Redis) DeadLetterCount(ctx context.Context, sessionID string) (int64, error) {
	n, err := r.client.LLen(ctx, deadLetterKey(sessionID)).Result()
	if err != nil {
		return 0, fmt.Errorf("count dead letters for %s: %w", sessionID, err)
	}
	return n, nil
}

// ReplayDeadLetter moves the oldest dead letter back into the mailbox.
//
// The attempt counter is cleared first, otherwise the replayed message would
// immediately exceed its budget again and land straight back in the dead
// letter — a replay that cannot possibly succeed.
//
// The replayed message is placed so it is served *after* anything the user
// sent while the failure was being investigated. A replay is a deliberate
// operator action taken once the cause is fixed; jumping ahead of newer
// messages would reorder the conversation, which is what the mailbox exists
// to prevent.
func (r *Redis) ReplayDeadLetter(ctx context.Context, sessionID string) (*types.InboundMessage, error) {
	raw, err := r.client.RPop(ctx, deadLetterKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // nothing to replay
	}
	if err != nil {
		return nil, fmt.Errorf("pop dead letter for %s: %w", sessionID, err)
	}

	var dl DeadLetter
	if err := json.Unmarshal([]byte(raw), &dl); err != nil {
		return nil, fmt.Errorf("unmarshal dead letter for %s: %w", sessionID, err)
	}
	if dl.Message == nil {
		return nil, fmt.Errorf("dead letter for %s carries no message", sessionID)
	}

	if err := r.ClearAttempts(ctx, dl.Message.RequestID); err != nil {
		return nil, err
	}

	// Push the message itself, not the dead-letter envelope: the mailbox is
	// read as InboundMessage, and an envelope there would fail to unmarshal
	// and stall the drain.
	payload, err := json.Marshal(dl.Message)
	if err != nil {
		return nil, fmt.Errorf("marshal replayed message: %w", err)
	}

	// LPush, matching how the mailbox is written.
	//
	// The mailbox is a list written with LPush and drained with RPop, so the
	// *head* of the list is the newest message and the tail is the next one
	// to be served. Using RPush here would place the replay at the read end
	// and serve it before messages the user sent while the failure was being
	// investigated — reordering the conversation, which is exactly what the
	// mailbox exists to prevent.
	if err := r.client.LPush(ctx, mailboxKey(sessionID), payload).Err(); err != nil {
		return nil, fmt.Errorf("replay into mailbox for %s: %w", sessionID, err)
	}
	return dl.Message, nil
}
