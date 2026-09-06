// 设计依据：docs/IM通道接入设计.md §9「长连接接入模式」
//                docs/多租户与节点部署设计.md §5.1.1「长连接通道的反向通路」

package types

import "context"

// StreamChannel is the inbound half of a channel that the platform connects
// *out* to, rather than one that calls in over HTTP.
//
// It exists because InboundChannel's three methods are all shaped around an
// HTTP callback — Verify takes a request, Decode parses one, Ack writes a
// response — and a long-lived connection has none of those. Rather than pass
// nil requests around, a stream channel implements this instead.
//
// Both kinds converge immediately afterwards: whatever Run produces goes
// through the same idempotency → mailbox → queue pipeline as a callback does.
// Two inbound paths would mean two implementations of the ordering and
// deduplication guarantees, which is the one thing this design must not have.
type StreamChannel interface {
	// Run holds the connection open until ctx is cancelled, handing each
	// received message to sink.
	//
	// This is the method that openclaw's Channel interface has always
	// declared and that nothing used to call. Implementations own their own
	// reconnect and heartbeat behaviour, because the retry semantics are
	// platform-specific: WeCom wants a 30-second ping and treats a missing
	// one as a dead connection.
	//
	// Returning a non-nil error means the connection is unrecoverable and the
	// binding should be abandoned until reconfigured. Returning ctx.Err() on
	// cancellation is the normal shutdown path and must not be logged as a
	// failure.
	Run(ctx context.Context, binding *ChannelBinding, sink MessageSink) error

	// Capabilities reports this channel's fixed traits.
	Capabilities() Capabilities
}

// MessageSink is how a StreamChannel hands an inbound message to the platform.
//
// It is a narrow interface rather than the Gateway type itself so that a
// channel implementation depends on nothing but types — the package that may
// not import any other internal package.
type MessageSink interface {
	// Accept runs one message through the inbound pipeline: idempotency
	// record, mailbox, queue.
	//
	// A duplicate is reported through AckInfo.Duplicate with a nil error,
	// exactly as the callback path does. An error means the idempotency
	// record could not be committed, in which case the channel must not treat
	// the message as handled: for a stream channel there is no platform-side
	// redelivery to fall back on, so the message is genuinely at risk and the
	// failure has to be surfaced.
	Accept(ctx context.Context, msg *InboundMessage) (AckInfo, error)
}

// StreamReply is one finished reply travelling back to the process that holds
// the connection.
//
// The reverse direction needs its own envelope because a stream reply carries
// something a normal outbound message does not: the platform's own correlation
// token. WeCom requires the reply to echo the req_id of the callback it
// answers, and that token originates inside the channel, crosses the queue to
// the Worker, and has to come back unchanged.
type StreamReply struct {
	// Routing. ChannelBindingID selects the outbox, and therefore which
	// connection — and which Gateway replica — this reply belongs to.
	Channel          string `json:"channel"`
	ChannelBindingID string `json:"channel_binding_id"`
	TenantID         string `json:"tenant_id"`

	// Target is the channel-specific destination: a user id for a direct
	// message, a chat id for a group.
	Target string `json:"target"`
	Scope  Scope  `json:"scope"`

	// CorrelationID is the platform's token for the request being answered,
	// opaque to the platform layer and passed through untouched.
	CorrelationID string `json:"correlation_id,omitempty"`

	// ExternalEventID identifies the inbound_events row this reply answers.
	// Carried because the delivery state machine is keyed by binding plus
	// external event id, and the holder — not the Worker — is what learns
	// whether the send succeeded.
	ExternalEventID string `json:"external_event_id"`

	// Text is the reply body.
	Text string `json:"text"`

	// Tracing, so a reply can be joined to the message that caused it.
	SessionID string `json:"session_id"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`

	// TraceContext carries the W3C headers, so the send span hangs off the
	// Worker's span instead of starting a second tree.
	TraceContext map[string]string `json:"trace_context,omitempty"`
}

// StreamOutbox carries replies from a Worker back to the Gateway replica
// holding the connection.
//
// This is the reverse of SessionDispatcher and exists for one reason: a
// long-connection reply can only leave through the socket it arrived on, and
// Gateway and Worker are forbidden from calling each other. Queueing keeps
// that rule intact — the Worker names a binding, never a replica, and never
// waits for one.
//
// The contract matches the forward direction: at-least-once, unordered,
// lossy. A lost reply is recovered the same way a lost hint is, by sweeping
// inbound_events rows that never reached a terminal state.
type StreamOutbox interface {
	// PushReply queues a reply for whichever process holds the binding's
	// connection.
	PushReply(ctx context.Context, reply *StreamReply) error

	// PopReply removes and returns the next reply for a binding, or nil when
	// none is waiting.
	PopReply(ctx context.Context, channelBindingID string) (*StreamReply, error)

	// ReplyLen reports how many replies are waiting, for observability and
	// for health checks that would otherwise not notice a stalled connection.
	ReplyLen(ctx context.Context, channelBindingID string) (int, error)
}

// StreamSender is the outbound half of a stream channel: it writes a reply
// into an already-established connection.
//
// Implemented by the channel and called only by the process that holds the
// connection, after it has taken the reply off the outbox.
type StreamSender interface {
	// SendReply writes the reply to the live connection for binding.
	//
	// It must fail rather than block indefinitely when no connection is
	// established: the caller marks the delivery failed and lets the sweeper
	// retry, which is preferable to a goroutine parked on a dead socket.
	SendReply(ctx context.Context, binding *ChannelBinding, reply *StreamReply) error
}

// ConnectionLease elects one holder per connection-limited resource.
//
// Needed because WeCom allows a bot only one live connection and kicks the
// previous one when a new subscription succeeds. Without election, two
// Gateway replicas would displace each other indefinitely.
//
// The shape mirrors SessionLease deliberately — same acquire/renew/release
// semantics, same "false is normal, not an error" convention, same TTL-as-
// failover-bound property. A separate interface rather than a reuse of
// SessionLease because the two are keyed differently (binding vs session) and
// want different TTLs: a connection lease should outlive a drain round.
type ConnectionLease interface {
	// AcquireConnection attempts to become the holder for a binding.
	// Returning false means another replica holds it; the caller becomes a
	// warm standby and retries later.
	AcquireConnection(ctx context.Context, channelBindingID, owner string) (bool, error)

	// RenewConnection extends the hold. A false return means the lease was
	// lost and the caller must close its connection: another replica has
	// taken over and the platform permits only one.
	RenewConnection(ctx context.Context, channelBindingID, owner string) (bool, error)

	// ReleaseConnection gives up the hold, a no-op unless the caller owns it.
	ReleaseConnection(ctx context.Context, channelBindingID, owner string) error
}

// StreamCapable reports whether a binding's capabilities put it in stream
// mode. Used at startup to decide which bindings need a Run loop, and in the
// Worker to decide whether a reply goes to the outbox or straight out over
// HTTP.
func (c Capabilities) StreamCapable() bool {
	return c.InboundMode == InboundModeStream
}
