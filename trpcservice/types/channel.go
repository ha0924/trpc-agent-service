// 设计依据：docs/IM通道接入设计.md §2「Channel 抽象」、§7「通道接入差异」
//                docs/框架复用与扩展.md §2.5「openclaw 中可复用的部分」

package types

import (
	"context"
	"net/http"

	openclawchannel "trpc.group/trpc-go/trpc-agent-go/openclaw/channel"
)

// Capabilities describes what one channel binding can and cannot do. Gateway
// and Worker branch on these values instead of on the channel name, so adding
// a channel does not mean editing the main flow.
//
// Stored as JSON in channel_bindings.capabilities.
type Capabilities struct {
	// InboundMode is "payload" when the callback carries the message itself,
	// or "fetch" when the callback is only a notification and the content
	// must be pulled separately. WeCom customer service is the fetch case.
	InboundMode InboundMode `json:"inbound_mode"`

	// SupportsPush reports whether the channel can deliver a message the user
	// did not just ask for. The whole ACK-then-reply-asynchronously design
	// depends on this; a channel without it must fall back to answering
	// within the callback and accept the timeout risk.
	SupportsPush bool `json:"supports_push"`

	// SupportsEdit reports whether an already-sent message can be edited.
	SupportsEdit bool `json:"supports_edit"`

	// MaxTextLength is the per-message limit; longer replies are split at
	// paragraph boundaries rather than truncated. Zero means no known limit.
	MaxTextLength int `json:"max_text_length"`

	// RateLimitPerMin caps outbound messages per minute for this binding.
	// Zero means unlimited.
	RateLimitPerMin int `json:"rate_limit_per_min"`
}

// InboundMode distinguishes the two shapes an inbound callback can take.
type InboundMode string

const (
	// InboundModePayload means the callback body contains the message.
	InboundModePayload InboundMode = "payload"
	// InboundModeFetch means the callback only signals that messages are
	// waiting and they must be pulled with a cursor.
	InboundModeFetch InboundMode = "fetch"
)

// AckInfo is what a channel may echo back in its immediate response.
//
// Duplicate is set when the idempotency record already existed. The platform
// still gets a successful ACK — a redelivery is not an error and must not be
// retried — but the field lets a caller distinguish the two cases when
// debugging, and lets a channel suppress a "received" notice for a message it
// already answered.
type AckInfo struct {
	RequestID string
	TraceID   string
	SessionID string
	Duplicate bool
}

// InboundChannel is the half of a channel that runs inside Gateway: it takes
// an untrusted HTTP callback and turns it into verified platform messages.
//
// It embeds openclaw's channel.Channel rather than redeclaring ID and Run, so
// the reuse relationship is checked by the compiler rather than asserted in
// prose. What openclaw does not model — verification, decoding, tenant
// binding and capability description — is what this interface adds.
type InboundChannel interface {
	openclawchannel.Channel

	// Verify authenticates the request against the binding's credentials.
	// It must run before Decode: an unverified body may be attacker-supplied.
	Verify(r *http.Request, binding *ChannelBinding) error

	// Decode turns a verified request into platform messages, decrypting if
	// the channel requires it.
	//
	// It returns a slice rather than a single message because fetch-mode
	// channels pull a batch per callback. Callers must handle zero messages:
	// a URL-verification handshake or an unrelated event type both decode to
	// nothing.
	Decode(ctx context.Context, r *http.Request, binding *ChannelBinding) ([]InboundMessage, error)

	// Ack writes the immediate response the platform expects. It is called
	// after the idempotency record is committed and before the message is
	// queued, so the platform stops retrying while the agent is still
	// working.
	//
	// The channel formats its own ACK because platforms disagree on what one
	// looks like: WeCom accepts an empty body, a webhook caller may expect
	// JSON. Info carries the identifiers a caller can echo back.
	Ack(w http.ResponseWriter, r *http.Request, binding *ChannelBinding, info AckInfo) error

	// Capabilities reports this channel's fixed traits. Per-binding overrides
	// come from channel_bindings.capabilities and take precedence.
	Capabilities() Capabilities
}

// OutboundChannel is the half that runs inside Worker: it delivers a finished
// reply back to the user.
//
// Inbound and outbound are separate interfaces because they live in different
// processes. Only Gateway is exposed publicly to receive callbacks, and only
// Worker knows when and what to reply.
type OutboundChannel interface {
	openclawchannel.Channel

	// Send delivers a reply to a channel-specific target. Target encoding is
	// the channel's own: a user id, a chat id, a group id.
	Send(ctx context.Context, target string, msg OutboundMessage, binding *ChannelBinding) error

	// Capabilities reports this channel's fixed traits.
	Capabilities() Capabilities
}

// Channel is a channel implementation that covers both directions. A single
// type usually implements both halves; the split exists so each process can
// depend on only the half it uses.
type Channel interface {
	InboundChannel
	OutboundChannel
}

// Registry resolves a channel implementation by its name, as stored in
// channel_bindings.channel.
type Registry interface {
	// Inbound returns the inbound half for the named channel.
	Inbound(name string) (InboundChannel, error)
	// Outbound returns the outbound half for the named channel.
	Outbound(name string) (OutboundChannel, error)
	// Names lists the registered channel names.
	Names() []string
}

// Compile-time proof that the platform interfaces genuinely extend openclaw's
// rather than merely resembling them: any Channel is usable wherever an
// openclaw Channel is expected.
var (
	_ interface{ openclawchannel.Channel } = (InboundChannel)(nil)
	_ interface{ openclawchannel.Channel } = (OutboundChannel)(nil)
)
