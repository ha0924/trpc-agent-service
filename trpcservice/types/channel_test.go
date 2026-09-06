package types

import "testing"

// These tests pin the separation of inbound and outbound mode.
//
// They were one field until a second long-connection channel made the
// conflation visible. WeCom's aibot couples them — it pushes inbound over a
// socket and requires replies on that same socket — so one field described
// both correctly and the coupling looked inherent. Telegram breaks it: long
// polling pulls inbound but replies over an ordinary HTTPS call any Worker
// can make.
//
// With one field, expressing "we dial out" would also route Telegram's
// replies through the outbox and start a per-bot election it does not need.

func TestOutboundModeDefaultsToDirect(t *testing.T) {
	// Empty must resolve to direct, so every binding written before this
	// field existed keeps its behaviour without a migration.
	if got := OutboundMode("").Resolved(); got != OutboundModeDirect {
		t.Errorf("empty outbound mode = %q, want direct", got)
	}
	if got := OutboundModeViaHolder.Resolved(); got != OutboundModeViaHolder {
		t.Errorf("via_holder should resolve to itself, got %q", got)
	}
}

func TestStreamCapableAnswersInboundOnly(t *testing.T) {
	cases := []struct {
		name string
		caps Capabilities
		want bool
	}{
		{
			name: "stream inbound needs a Run loop",
			caps: Capabilities{InboundMode: InboundModeStream},
			want: true,
		},
		{
			// The distinguishing case: dialling out for inbound while
			// replying directly. StreamCapable must still be true — a Run
			// loop is needed — even though replies do not use the socket.
			name: "stream inbound with direct replies still needs a Run loop",
			caps: Capabilities{
				InboundMode:  InboundModeStream,
				OutboundMode: OutboundModeDirect,
			},
			want: true,
		},
		{
			name: "payload inbound does not",
			caps: Capabilities{InboundMode: InboundModePayload},
			want: false,
		},
		{
			name: "fetch inbound does not",
			caps: Capabilities{InboundMode: InboundModeFetch},
			want: false,
		},
		{
			// An unset mode must not be treated as stream: a binding with no
			// capabilities would otherwise have a connection opened for it.
			name: "unset inbound does not",
			caps: Capabilities{},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.caps.StreamCapable(); got != c.want {
				t.Errorf("StreamCapable() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestRepliesViaHolderAnswersOutboundOnly(t *testing.T) {
	cases := []struct {
		name string
		caps Capabilities
		want bool
	}{
		{
			name: "explicit via_holder routes through the outbox",
			caps: Capabilities{
				InboundMode:  InboundModeStream,
				OutboundMode: OutboundModeViaHolder,
			},
			want: true,
		},
		{
			// This is the case the split exists for. Telegram-shaped: dials
			// out for inbound, replies directly. Routing it through the
			// outbox would add a hop and an election for nothing.
			name: "stream inbound alone does not imply via_holder",
			caps: Capabilities{InboundMode: InboundModeStream},
			want: false,
		},
		{
			name: "callback bindings reply directly",
			caps: Capabilities{InboundMode: InboundModePayload},
			want: false,
		},
		{
			name: "explicit direct replies directly",
			caps: Capabilities{
				InboundMode:  InboundModeStream,
				OutboundMode: OutboundModeDirect,
			},
			want: false,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.caps.RepliesViaHolder(); got != c.want {
				t.Errorf("RepliesViaHolder() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestTheTwoDimensionsAreIndependent(t *testing.T) {
	// All four combinations must be expressible. Before the split, two of
	// these could not be said at all.
	combos := []struct {
		name      string
		caps      Capabilities
		runLoop   bool
		viaOutbox bool
	}{
		{
			name:      "webhook in, direct out (mock, wecom callback)",
			caps:      Capabilities{InboundMode: InboundModePayload},
			runLoop:   false,
			viaOutbox: false,
		},
		{
			name: "socket in, socket out (wecom aibot)",
			caps: Capabilities{
				InboundMode:  InboundModeStream,
				OutboundMode: OutboundModeViaHolder,
			},
			runLoop:   true,
			viaOutbox: true,
		},
		{
			name: "socket in, direct out (telegram long polling)",
			caps: Capabilities{
				InboundMode:  InboundModeStream,
				OutboundMode: OutboundModeDirect,
			},
			runLoop:   true,
			viaOutbox: false,
		},
		{
			name:      "fetch in, direct out (wechat customer service)",
			caps:      Capabilities{InboundMode: InboundModeFetch},
			runLoop:   false,
			viaOutbox: false,
		},
	}
	for _, c := range combos {
		t.Run(c.name, func(t *testing.T) {
			if got := c.caps.StreamCapable(); got != c.runLoop {
				t.Errorf("StreamCapable() = %v, want %v", got, c.runLoop)
			}
			if got := c.caps.RepliesViaHolder(); got != c.viaOutbox {
				t.Errorf("RepliesViaHolder() = %v, want %v", got, c.viaOutbox)
			}
		})
	}
}
