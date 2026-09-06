// 设计依据：docs/IM通道接入设计.md §9.3「回复路径反转」、§9.4「单连接约束与 per-bot 选主」
//                docs/多租户与节点部署设计.md §5.1.1「长连接通道的反向通路」

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

// Key prefixes for the reverse direction. Kept under the same namespace as
// the forward ones so the whole scheduler state can be inspected together.
const (
	prefixOutbox    = "agent:outbox:"
	prefixConnLease = "agent:connlease:"
)

var (
	_ types.StreamOutbox    = (*Redis)(nil)
	_ types.ConnectionLease = (*Redis)(nil)
)

func outboxKey(bindingID string) string { return prefixOutbox + bindingID }

func connLeaseKey(bindingID string) string { return prefixConnLease + bindingID }

// ---------------------------------------------------------------------------
// StreamOutbox
// ---------------------------------------------------------------------------

// PushReply queues a reply for whichever process holds the binding's
// connection.
//
// Appended on the left and drained from the right, mirroring the mailbox, so
// replies leave in the order the Worker produced them. Ordering here matters
// less than in the mailbox — a session is drained by one Worker at a time
// anyway — but arriving out of order would still be visible to the user when
// a long reply is split into several messages.
func (r *Redis) PushReply(ctx context.Context, reply *types.StreamReply) error {
	if reply == nil {
		return errors.New("push reply: nil reply")
	}
	if reply.ChannelBindingID == "" {
		// Without a binding id there is no outbox to route to, and a reply
		// pushed to an empty key would be invisible rather than merely late.
		return errors.New("push reply: reply has no channel binding id")
	}
	payload, err := json.Marshal(reply)
	if err != nil {
		return fmt.Errorf("marshal stream reply: %w", err)
	}
	if err := r.client.LPush(ctx, outboxKey(reply.ChannelBindingID), payload).Err(); err != nil {
		return fmt.Errorf("push reply for binding %s: %w", reply.ChannelBindingID, err)
	}
	return nil
}

// PopReply removes and returns the next reply, or nil when none is waiting.
func (r *Redis) PopReply(ctx context.Context, channelBindingID string) (*types.StreamReply, error) {
	raw, err := r.client.RPop(ctx, outboxKey(channelBindingID)).Result()
	if errors.Is(err, redis.Nil) {
		return nil, nil // empty is the normal idle state, not an error
	}
	if err != nil {
		return nil, fmt.Errorf("pop reply for binding %s: %w", channelBindingID, err)
	}

	var reply types.StreamReply
	if err := json.Unmarshal([]byte(raw), &reply); err != nil {
		return nil, fmt.Errorf("unmarshal stream reply for %s: %w", channelBindingID, err)
	}
	return &reply, nil
}

// ReplyLen reports how many replies are waiting.
//
// Worth exposing on the health endpoint: a growing outbox with a live
// connection means the sender is stuck, which no other signal would reveal.
func (r *Redis) ReplyLen(ctx context.Context, channelBindingID string) (int, error) {
	n, err := r.client.LLen(ctx, outboxKey(channelBindingID)).Result()
	if err != nil {
		return 0, fmt.Errorf("outbox length for %s: %w", channelBindingID, err)
	}
	return int(n), nil
}

// ---------------------------------------------------------------------------
// ConnectionLease
// ---------------------------------------------------------------------------

// connLeaseTTL is deliberately longer than the session lease TTL.
//
// A session lease covers one drain round; a connection lease covers a live
// socket that should survive brief Redis latency or a slow renew without the
// binding being handed to another replica. Too short and replicas would take
// turns kicking each other's connection — exactly the failure the lease
// exists to prevent.
const connLeaseTTL = 45 * time.Second

// AcquireConnection attempts to become the holder for a binding.
//
// Returning false is the normal outcome for every replica but one, and means
// "another replica holds the connection". Callers become warm standbys and
// retry; it must not be logged as an error.
func (r *Redis) AcquireConnection(ctx context.Context, channelBindingID, owner string) (bool, error) {
	ok, err := r.client.SetNX(ctx, connLeaseKey(channelBindingID), owner, connLeaseTTL).Result()
	if err != nil {
		return false, fmt.Errorf("acquire connection lease for %s: %w", channelBindingID, err)
	}
	return ok, nil
}

// RenewConnection extends the hold, reusing the session lease's Lua script
// because the check-then-act race and its fix are identical: without
// atomicity a replica could extend a lease that has already been taken over
// and end up holding a connection the platform has already replaced.
func (r *Redis) RenewConnection(ctx context.Context, channelBindingID, owner string) (bool, error) {
	res, err := renewScript.Run(ctx, r.client,
		[]string{connLeaseKey(channelBindingID)}, owner, connLeaseTTL.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("renew connection lease for %s: %w", channelBindingID, err)
	}
	return res == 1, nil
}

// ReleaseConnection gives up the hold. Releasing someone else's lease is a
// no-op, so a replica that lost its lease and only noticed later cannot evict
// the replica that legitimately took over.
func (r *Redis) ReleaseConnection(ctx context.Context, channelBindingID, owner string) error {
	if err := releaseScript.Run(ctx, r.client,
		[]string{connLeaseKey(channelBindingID)}, owner).Err(); err != nil && !isRedisNil(err) {
		return fmt.Errorf("release connection lease for %s: %w", channelBindingID, err)
	}
	return nil
}

// ConnectionOwner reports who currently holds a binding's connection, for
// diagnostics: "which replica is my bot connected from" is otherwise
// unanswerable.
func (r *Redis) ConnectionOwner(ctx context.Context, channelBindingID string) (string, error) {
	owner, err := r.client.Get(ctx, connLeaseKey(channelBindingID)).Result()
	if errors.Is(err, redis.Nil) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("read connection owner for %s: %w", channelBindingID, err)
	}
	return owner, nil
}

// ConnectionLeaseTTL exposes the TTL so callers can pick a renew interval
// from it rather than hardcoding a second value that could drift out of step.
func ConnectionLeaseTTL() time.Duration { return connLeaseTTL }
