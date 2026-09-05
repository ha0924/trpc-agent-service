// 设计依据：docs/多租户与节点部署设计.md §5「调度模型」
//                docs/dev/技术栈说明.md §6「消息队列：待定」

package types

import "context"

// SessionHint tells a Worker that a session has messages waiting. It is
// deliberately tiny: the queue carries only a pointer, never the message
// itself. The payload lives in the session mailbox and the durable record
// lives in inbound_events, so losing a hint costs nothing that a sweep of
// stale processing records cannot recover.
type SessionHint struct {
	TenantID     string `json:"tenant_id"`
	AgentAppID   string `json:"agent_app_id"`
	AgentVersion string `json:"agent_version"`
	SessionID    string `json:"session_id"`

	// TraceID lets the Worker continue the trace the Gateway started. Without
	// it the two processes' logs cannot be joined, which is the one
	// observability guarantee phase one must meet.
	TraceID string `json:"trace_id"`

	// TraceContext carries the W3C traceparent and tracestate headers.
	//
	// TraceID above is enough to join log lines, but not to join spans: a
	// child span needs its parent's span id and the sampling decision, both of
	// which travel here. Without it the Worker starts a new root span and one
	// message produces two disconnected traces instead of one tree.
	TraceContext map[string]string `json:"trace_context,omitempty"`
}

// SessionDispatcher moves pending-session hints from Gateway to Worker.
//
// The contract is intentionally the weakest that still works:
//
//	at-least-once delivery
//	no ordering guarantee
//	no durability guarantee
//
// Ordering is provided by the session lease and mailbox, not by the queue.
// Durability is provided by writing the idempotency record before ACK, not by
// the queue. Keeping the contract this weak is what allows Redis List, Redis
// Stream, Kafka, RocketMQ or Pulsar to be swapped in without touching callers
// — and it is why ordered-message features of those brokers must not be
// enabled, since partition-based ordering would cap concurrency at the
// partition count and make rebalances interrupt long-running work.
type SessionDispatcher interface {
	// Publish announces that a session has work waiting.
	Publish(ctx context.Context, hint SessionHint) error

	// Subscribe returns a stream of hints. The channel closes when ctx is
	// cancelled. Implementations may deliver the same hint more than once;
	// consumers must be safe against that, which they are because acting on a
	// hint requires winning the session lease first.
	Subscribe(ctx context.Context) (<-chan SessionHint, error)

	// Close releases the underlying connection.
	Close() error
}

// SessionMailbox holds the messages waiting for one session, in arrival
// order. It is the structure that actually preserves ordering.
type SessionMailbox interface {
	// Push appends a message to the session's mailbox.
	Push(ctx context.Context, sessionID string, msg *InboundMessage) error

	// Pop removes and returns the next message, or nil when the mailbox is
	// empty. A Worker holding the lease drains with Pop until it returns nil.
	Pop(ctx context.Context, sessionID string) (*InboundMessage, error)

	// Len reports how many messages are waiting, for observability.
	Len(ctx context.Context, sessionID string) (int, error)
}

// SessionLease guarantees that at most one Worker processes a given session
// at a time.
//
// The lease is held per drain round, not for the lifetime of the
// conversation: a Worker acquires it, empties the mailbox, then releases it,
// so the next round may land on any healthy Worker. This is what makes
// Workers interchangeable and sticky sessions unnecessary.
//
// Its TTL doubles as the failure-recovery time: a Worker that dies holding a
// lease blocks its session only until the TTL expires.
type SessionLease interface {
	// Acquire attempts to take the lease. It returns false without error when
	// another Worker holds it — that is the normal, expected outcome and must
	// not be logged as a failure.
	Acquire(ctx context.Context, sessionID, owner string) (bool, error)

	// Renew extends the lease. A Worker whose Renew returns false has lost
	// the lease and must stop writing immediately rather than finish the
	// current round, because another Worker is already processing the
	// session.
	Renew(ctx context.Context, sessionID, owner string) (bool, error)

	// Release drops the lease. It must be a no-op when the caller is not the
	// current owner, so a slow Worker cannot release a lease that has already
	// been taken over.
	Release(ctx context.Context, sessionID, owner string) error
}
