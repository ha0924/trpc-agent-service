// 设计依据：docs/多租户与节点部署设计.md §5「调度模型」
//                docs/dev/技术栈说明.md §6「消息队列：待定」

// Package scheduler implements session ordering on Redis: the queue that
// hands work to Workers, the per-session mailbox that preserves arrival
// order, and the lease that admits one Worker at a time.
//
// The division of labour matters. The queue only says "this session has
// work"; it may duplicate, reorder or drop hints. Ordering comes from the
// mailbox, mutual exclusion from the lease, and durability from the
// inbound_events row written before the ACK. Keeping the queue's contract
// this weak is what allows it to be replaced with Kafka or anything else
// without touching callers.
package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Key prefixes. Everything the scheduler owns lives under one namespace so it
// can be inspected and flushed without touching other Redis users.
const (
	prefixMailbox = "agent:mailbox:"
	prefixLease   = "agent:lease:"
)

// Redis implements the dispatcher, mailbox and lease against one connection.
type Redis struct {
	client   *redis.Client
	queueKey string
	leaseTTL time.Duration
}

// Compile-time proof that the Redis implementation satisfies all three
// contracts. Without these, a renamed method would only fail at the call site.
var (
	_ types.SessionDispatcher = (*Redis)(nil)
	_ types.SessionMailbox    = (*Redis)(nil)
	_ types.SessionLease      = (*Redis)(nil)
)

// New connects to Redis and verifies the connection.
func New(ctx context.Context, rc config.RedisConfig, sc config.SchedulerConfig) (*Redis, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     rc.Addr,
		Password: rc.Password,
		DB:       rc.DB,
	})
	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("ping redis at %s: %w", rc.Addr, err)
	}
	return &Redis{client: client, queueKey: sc.QueueKey, leaseTTL: sc.LeaseTTL}, nil
}

// NewWithClient wraps an existing client, for tests.
func NewWithClient(client *redis.Client, queueKey string, leaseTTL time.Duration) *Redis {
	return &Redis{client: client, queueKey: queueKey, leaseTTL: leaseTTL}
}

// Client exposes the connection for callers needing their own commands.
func (r *Redis) Client() *redis.Client { return r.client }

// Ping checks liveness, for health endpoints.
func (r *Redis) Ping(ctx context.Context) error { return r.client.Ping(ctx).Err() }

// Close releases the connection.
func (r *Redis) Close() error { return r.client.Close() }

// ---------------------------------------------------------------------------
// SessionDispatcher
// ---------------------------------------------------------------------------

// Publish announces that a session has work waiting.
func (r *Redis) Publish(ctx context.Context, hint types.SessionHint) error {
	payload, err := json.Marshal(hint)
	if err != nil {
		return fmt.Errorf("marshal session hint: %w", err)
	}
	if err := r.client.LPush(ctx, r.queueKey, payload).Err(); err != nil {
		return fmt.Errorf("publish hint for session %s: %w", hint.SessionID, err)
	}
	return nil
}

// Subscribe streams hints until ctx is cancelled.
//
// A blocking pop with a short timeout is used rather than an indefinite one so
// the loop notices cancellation promptly at shutdown instead of sitting on a
// connection until Redis times out.
func (r *Redis) Subscribe(ctx context.Context) (<-chan types.SessionHint, error) {
	out := make(chan types.SessionHint)

	go func() {
		defer close(out)
		for {
			if ctx.Err() != nil {
				return
			}

			res, err := r.client.BRPop(ctx, time.Second, r.queueKey).Result()
			if errors.Is(err, redis.Nil) {
				continue // idle timeout, poll again
			}
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				// Redis hiccup: back off briefly rather than spinning. Losing
				// a hint is acceptable — inbound_events remains the durable
				// record and stuck rows are swept separately.
				time.Sleep(200 * time.Millisecond)
				continue
			}
			if len(res) != 2 {
				continue
			}

			var hint types.SessionHint
			if err := json.Unmarshal([]byte(res[1]), &hint); err != nil {
				continue // malformed hint: drop it, the mailbox still holds the work
			}

			select {
			case out <- hint:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out, nil
}

// QueueLen reports pending hints, for observability.
func (r *Redis) QueueLen(ctx context.Context) (int64, error) {
	return r.client.LLen(ctx, r.queueKey).Result()
}

// ---------------------------------------------------------------------------
// SessionMailbox
// ---------------------------------------------------------------------------

func mailboxKey(sessionID string) string { return prefixMailbox + sessionID }

// Push appends a message to the session's mailbox.
//
// The mailbox, not the queue, is what preserves order: messages are appended
// on the left and drained from the right, so a Worker sees them in arrival
// order regardless of how the queue shuffled the hints.
func (r *Redis) Push(ctx context.Context, sessionID string, msg *types.InboundMessage) error {
	payload, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal mailbox message: %w", err)
	}
	if err := r.client.LPush(ctx, mailboxKey(sessionID), payload).Err(); err != nil {
		return fmt.Errorf("push to mailbox %s: %w", sessionID, err)
	}
	return nil
}

// Pop removes and returns the next message, or nil when the mailbox is empty.
func (r *Redis) Pop(ctx context.Context, sessionID string) (*types.InboundMessage, error) {
	raw, err := r.client.RPop(ctx, mailboxKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // empty is a normal end-of-drain condition, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("pop from mailbox %s: %w", sessionID, err)
	}

	var msg types.InboundMessage
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		return nil, fmt.Errorf("unmarshal mailbox message for %s: %w", sessionID, err)
	}
	return &msg, nil
}

// Len reports how many messages are waiting.
func (r *Redis) Len(ctx context.Context, sessionID string) (int, error) {
	n, err := r.client.LLen(ctx, mailboxKey(sessionID)).Result()
	if err != nil {
		return 0, fmt.Errorf("mailbox length for %s: %w", sessionID, err)
	}
	return int(n), nil
}

// ---------------------------------------------------------------------------
// SessionLease
// ---------------------------------------------------------------------------

func leaseKey(sessionID string) string { return prefixLease + sessionID }

// renewScript extends the lease only if the caller still owns it.
//
// Check-then-act in application code would race: the lease could expire and be
// taken between the GET and the EXPIRE, and the original holder would then
// extend someone else's lease. Lua runs both steps atomically.
var renewScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("PEXPIRE", KEYS[1], ARGV[2])
end
return 0
`)

// releaseScript deletes the lease only if the caller still owns it, so a slow
// Worker cannot release a lease that has already been taken over.
var releaseScript = redis.NewScript(`
if redis.call("GET", KEYS[1]) == ARGV[1] then
  return redis.call("DEL", KEYS[1])
end
return 0
`)

// Acquire attempts to take the session lease.
//
// Returning false is the normal outcome when another Worker holds the lease —
// with at-least-once delivery, several Workers routinely race for the same
// hint. Callers must treat it as "not my turn", not as a failure.
func (r *Redis) Acquire(ctx context.Context, sessionID, owner string) (bool, error) {
	ok, err := r.client.SetNX(ctx, leaseKey(sessionID), owner, r.leaseTTL).Result()
	if err != nil {
		return false, fmt.Errorf("acquire lease for %s: %w", sessionID, err)
	}
	return ok, nil
}

// Renew extends the lease.
//
// A false return means the lease was lost — the TTL elapsed and another Worker
// took over. The caller must stop writing immediately rather than finish its
// round, because a second Worker is already processing this session.
func (r *Redis) Renew(ctx context.Context, sessionID, owner string) (bool, error) {
	res, err := renewScript.Run(ctx, r.client,
		[]string{leaseKey(sessionID)}, owner, r.leaseTTL.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("renew lease for %s: %w", sessionID, err)
	}
	return res == 1, nil
}

// Release drops the lease. Releasing a lease owned by someone else is a no-op.
func (r *Redis) Release(ctx context.Context, sessionID, owner string) error {
	if err := releaseScript.Run(ctx, r.client,
		[]string{leaseKey(sessionID)}, owner).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("release lease for %s: %w", sessionID, err)
	}
	return nil
}

// LeaseOwner reports who currently holds the lease, for diagnostics.
func (r *Redis) LeaseOwner(ctx context.Context, sessionID string) (string, error) {
	owner, err := r.client.Get(ctx, leaseKey(sessionID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read lease owner for %s: %w", sessionID, err)
	}
	return owner, nil
}
