// 设计依据：docs/IM通道接入设计.md §9「长连接接入模式」、§9.6「入站必须汇入同一条流水线」

// Package mockstream is a stream-mode Channel for validating the
// long-connection path without a real IM platform.
//
// It exists because the two hard parts of long-connection support are not the
// wire protocol — they are the reversed reply path and the single-connection
// election, and both are platform-independent. Proving them against a mock
// first means a WeCom bug is a WeCom bug, rather than something that could
// equally be a flaw in the supervisor.
//
// The channel is driven by a Go channel of feed items instead of a socket:
// Run blocks reading from it exactly as a real implementation blocks reading
// from a connection, and SendReply appends to a slice a test can inspect
// where a real one would write a frame.
package mockstream

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Name is the value stored in channel_bindings.channel.
const Name = "mockstream"

// Feed is how a test injects inbound messages, standing in for the socket a
// real channel would read.
type Feed chan *types.InboundMessage

// Channel implements the stream half of the platform Channel contract.
type Channel struct {
	feed Feed

	mu sync.Mutex
	// sent records delivered replies so a test can assert on what the user
	// would have seen, including ordering.
	sent []types.StreamReply
	// runs counts how many times Run has been entered, which is what proves
	// election works: with N replicas supervising one binding, exactly one
	// should ever be running.
	runs int
	// live tracks whether a connection is currently open, so SendReply can
	// fail when there is none — mirroring a real channel, where writing to a
	// closed socket must not silently succeed.
	live bool
	// failSend makes SendReply fail, to exercise the delivery-failure path.
	failSend bool
}

var (
	_ types.StreamChannel = (*Channel)(nil)
	_ types.StreamSender  = (*Channel)(nil)
)

// New builds a stream mock reading from feed.
func New(feed Feed) *Channel {
	return &Channel{feed: feed}
}

// ID identifies the channel. Part of openclaw's Channel interface.
func (c *Channel) ID() string { return Name }

// Capabilities reports stream-mode traits.
//
// InboundModeStream is what makes the supervisor pick this binding up and
// what makes the Worker route replies to the outbox instead of sending them
// directly — the two behaviours that distinguish this mode.
func (c *Channel) Capabilities() types.Capabilities {
	return types.Capabilities{
		InboundMode: types.InboundModeStream,
		// This mock exists to exercise the reversed reply path, so it declares
		// via_holder explicitly.
		OutboundMode:    types.OutboundModeViaHolder,
		SupportsPush:    true,
		SupportsEdit:    true,
		MaxTextLength:   2048,
		RateLimitPerMin: 30,
	}
}

// Run reads from the feed and hands each message to the sink, until ctx ends.
//
// The sink is the platform's real inbound pipeline, so a message injected
// here gets the same idempotency record, the same mailbox ordering and the
// same queue hint as an HTTP callback would. That is the property worth
// testing: not that the mock works, but that stream mode did not grow a
// second pipeline.
func (c *Channel) Run(ctx context.Context, binding *types.ChannelBinding, sink types.MessageSink) error {
	if binding == nil {
		return errors.New("mockstream: run requires a binding")
	}
	if sink == nil {
		return errors.New("mockstream: run requires a sink")
	}

	c.mu.Lock()
	c.runs++
	c.live = true
	c.mu.Unlock()

	defer func() {
		c.mu.Lock()
		c.live = false
		c.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			// Normal shutdown, or the lease was lost. Either way the caller
			// distinguishes this from a real failure by the error value.
			return ctx.Err()

		case msg, ok := <-c.feed:
			if !ok {
				return nil // feed closed: connection ended cleanly
			}
			if msg == nil {
				continue
			}
			if _, err := sink.Accept(ctx, msg); err != nil {
				// A stream channel has no platform-side redelivery to lean
				// on, so a rejected message is genuinely lost. Surfacing the
				// error ends the connection and lets the supervisor
				// reconnect, which at least makes the loss visible.
				return fmt.Errorf("mockstream: accept message: %w", err)
			}
		}
	}
}

// SendReply records a reply, standing in for writing a frame.
func (c *Channel) SendReply(
	ctx context.Context,
	binding *types.ChannelBinding,
	reply *types.StreamReply,
) error {
	if reply == nil {
		return errors.New("mockstream: nil reply")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.failSend {
		return errors.New("mockstream: send failed as configured")
	}
	if !c.live {
		// A real channel cannot write to a connection it does not hold, and
		// pretending otherwise would hide the very bug this mock exists to
		// catch: a reply routed to a replica that no longer has the socket.
		return errors.New("mockstream: no live connection")
	}

	c.sent = append(c.sent, *reply)
	return nil
}

// Sent returns a copy of the delivered replies.
func (c *Channel) Sent() []types.StreamReply {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]types.StreamReply, len(c.sent))
	copy(out, c.sent)
	return out
}

// Runs reports how many times Run was entered, for asserting that election
// admitted exactly one holder.
func (c *Channel) Runs() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runs
}

// Live reports whether a connection is currently open.
func (c *Channel) Live() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.live
}

// SetFailSend toggles send failure, to exercise the delivery-failure path.
func (c *Channel) SetFailSend(fail bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.failSend = fail
}

// WaitForSent blocks until at least n replies have been sent or the timeout
// elapses, returning whether the count was reached.
//
// Polling rather than signalling keeps the mock's API to plain method calls;
// the interval is short enough that a test does not pay for it noticeably.
func (c *Channel) WaitForSent(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(c.Sent()) >= n {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return len(c.Sent()) >= n
}
