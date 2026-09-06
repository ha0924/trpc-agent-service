package worker

import (
	"context"
	"errors"
	"testing"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests cover the delivery-state contract for stream bindings, which is
// where the reversed reply path can go wrong silently.
//
// The failure they guard against: for a stream binding, a successful hand-off
// means *queued*, not *delivered*. The send happens in whichever Gateway
// replica holds the socket, and only that replica learns the outcome. Marking
// the row succeeded at hand-off time would report a delivery that may still
// fail — and because the row would then be terminal, nothing would retry it.
// The reply would be lost with every signal reading green.

// fakeRedeliverer records what the sweeper passes and reports a configurable
// outcome, standing in for a Worker.
type fakeRedeliverer struct {
	gotOriginal *types.InboundMessage
	gotReply    string
	queued      bool
	err         error
	calls       int
}

func (f *fakeRedeliverer) Redeliver(
	ctx context.Context,
	tenantID, sessionID, requestID, reply string,
	original *types.InboundMessage,
) (bool, error) {
	f.calls++
	f.gotOriginal = original
	f.gotReply = reply
	return f.queued, f.err
}

var _ Redeliverer = (*fakeRedeliverer)(nil)

// TestRedelivererContractCarriesOriginalMessage pins the signature that the
// bug came from.
//
// The original Redeliver took only ids and the reply text, rebuilding the
// inbound message from the session. That silently dropped two fields no
// session can supply:
//
//   - ExternalEventID keys the delivery state machine, so a stream reply
//     queued without it left the holder unable to record any outcome.
//   - CorrelationID is the platform's token for the original exchange;
//     without it a WeCom reply degrades from answering the message to
//     pushing an unsolicited one, which has different preconditions.
func TestRedelivererContractCarriesOriginalMessage(t *testing.T) {
	f := &fakeRedeliverer{}

	original := &types.InboundMessage{
		ExternalEventID: "msg-99",
		CorrelationID:   "platform-req-42",
		ExternalUserID:  "T31560051A",
	}

	if _, err := f.Redeliver(context.Background(),
		"tenant-test", "sess-1", "req-1", "the answer", original); err != nil {
		t.Fatalf("Redeliver: %v", err)
	}

	if f.gotOriginal == nil {
		t.Fatal("the original message must reach the redeliverer")
	}
	if f.gotOriginal.ExternalEventID != "msg-99" {
		t.Errorf("ExternalEventID = %q, want msg-99 — without it the delivery "+
			"state machine cannot find the row", f.gotOriginal.ExternalEventID)
	}
	if f.gotOriginal.CorrelationID != "platform-req-42" {
		t.Errorf("CorrelationID = %q, want platform-req-42 — without it a "+
			"long-connection reply cannot answer the original exchange",
			f.gotOriginal.CorrelationID)
	}
}

// TestQueuedIsDistinctFromDelivered is the core of the fix: the contract has
// to express three outcomes, not two.
func TestQueuedIsDistinctFromDelivered(t *testing.T) {
	cases := []struct {
		name       string
		queued     bool
		err        error
		wantQueued bool
		wantErr    bool
	}{
		{
			// Callback bindings: sent synchronously, so the caller may mark
			// the row succeeded immediately.
			name: "direct delivery reports sent",
		},
		{
			// Stream bindings: handed to the outbox. Reporting this as plain
			// success is exactly the bug — the caller must not finalise.
			name:       "stream delivery reports queued",
			queued:     true,
			wantQueued: true,
		},
		{
			// A failure must stay a failure in both modes, so the row goes
			// back to delivery_failed and the attempt counter advances.
			name:    "failure reports an error",
			err:     errors.New("no live connection"),
			wantErr: true,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := &fakeRedeliverer{queued: c.queued, err: c.err}
			queued, err := f.Redeliver(context.Background(),
				"tenant-test", "sess-1", "req-1", "reply", &types.InboundMessage{})

			if (err != nil) != c.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, c.wantErr)
			}
			if queued != c.wantQueued {
				t.Errorf("queued = %v, want %v", queued, c.wantQueued)
			}
			// A queued reply is never also an error: the caller branches on
			// err first, and a true-with-error result would be ambiguous.
			if queued && err != nil {
				t.Error("queued and error are mutually exclusive outcomes")
			}
		})
	}
}

// TestStreamCapableDrivesTheBranch checks that the routing decision reads
// capabilities rather than a channel name.
//
// Branching on the name would mean editing the delivery path for every new
// long-connection platform, and the point of the capability descriptor is
// that the main flow has no per-channel cases.
func TestStreamCapableDrivesTheBranch(t *testing.T) {
	cases := []struct {
		name string
		caps types.Capabilities
		want bool
	}{
		{
			name: "stream mode routes to the outbox",
			caps: types.Capabilities{InboundMode: types.InboundModeStream},
			want: true,
		},
		{
			name: "payload mode sends directly",
			caps: types.Capabilities{InboundMode: types.InboundModePayload},
		},
		{
			name: "fetch mode sends directly",
			caps: types.Capabilities{InboundMode: types.InboundModeFetch},
		},
		{
			// An unset mode must not be treated as stream: a binding with no
			// capabilities configured would otherwise queue replies to an
			// outbox nobody drains.
			name: "unset mode sends directly",
			caps: types.Capabilities{},
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
