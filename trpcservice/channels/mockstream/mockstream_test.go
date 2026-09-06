package mockstream

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// recordingSink captures what Run hands to the pipeline, standing in for the
// Gateway. Using the real types.MessageSink interface is the point: if the
// signature ever drifts from what a channel can supply, this stops compiling.
type recordingSink struct {
	got  []*types.InboundMessage
	err  error
	dupe bool
}

func (s *recordingSink) Accept(ctx context.Context, msg *types.InboundMessage) (types.AckInfo, error) {
	if s.err != nil {
		return types.AckInfo{}, s.err
	}
	s.got = append(s.got, msg)
	return types.AckInfo{
		RequestID: msg.RequestID,
		SessionID: "sess-1",
		Duplicate: s.dupe,
	}, nil
}

var _ types.MessageSink = (*recordingSink)(nil)

func TestCapabilitiesPutBindingInStreamMode(t *testing.T) {
	c := New(make(Feed))

	// The whole supervisor and the Worker's reply routing branch on this one
	// value. If it were not stream, the binding would silently behave like a
	// webhook one: no Run loop, and replies sent from a process with no
	// socket.
	if !c.Capabilities().StreamCapable() {
		t.Fatal("mockstream must report stream mode")
	}
	if c.Capabilities().InboundMode != types.InboundModeStream {
		t.Fatalf("InboundMode = %q, want stream", c.Capabilities().InboundMode)
	}
}

func TestRunFeedsMessagesToTheSinkInOrder(t *testing.T) {
	feed := make(Feed, 3)
	c := New(feed)
	sink := &recordingSink{}
	binding := &types.ChannelBinding{
		ChannelBindingID: "bind-1",
		TenantID:         "tenant-demo",
		AgentAppID:       "app-1",
		Channel:          Name,
	}

	for _, id := range []string{"m1", "m2", "m3"} {
		feed <- &types.InboundMessage{ExternalEventID: id, Text: id, RequestID: "req-" + id}
	}
	close(feed)

	if err := c.Run(context.Background(), binding, sink); err != nil {
		t.Fatalf("Run: %v", err)
	}

	if len(sink.got) != 3 {
		t.Fatalf("sink saw %d messages, want 3", len(sink.got))
	}
	// Order matters even here: the mailbox preserves arrival order, but only
	// if the channel hands messages over in the order it received them.
	for i, want := range []string{"m1", "m2", "m3"} {
		if sink.got[i].ExternalEventID != want {
			t.Errorf("message %d = %q, want %q", i, sink.got[i].ExternalEventID, want)
		}
	}
}

func TestRunReturnsWhenContextIsCancelled(t *testing.T) {
	c := New(make(Feed))
	ctx, cancel := context.WithCancel(context.Background())
	binding := &types.ChannelBinding{ChannelBindingID: "bind-1", TenantID: "tenant-demo"}

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, binding, &recordingSink{}) }()

	// Wait for the connection to come up before cancelling, or the test
	// could pass without Run ever having blocked.
	deadline := time.Now().Add(time.Second)
	for !c.Live() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !c.Live() {
		t.Fatal("Run did not open a connection")
	}

	cancel()

	select {
	case err := <-done:
		// Cancellation is the normal shutdown and the lease-loss path. It has
		// to be distinguishable from a real failure, or the supervisor would
		// log every graceful stop as an error.
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	if c.Live() {
		t.Fatal("connection should be marked closed after Run returns")
	}
}

func TestRunFailsWhenTheSinkRejects(t *testing.T) {
	feed := make(Feed, 1)
	c := New(feed)
	sink := &recordingSink{err: errors.New("idempotency write failed")}
	binding := &types.ChannelBinding{ChannelBindingID: "bind-1", TenantID: "tenant-demo"}

	feed <- &types.InboundMessage{ExternalEventID: "m1", Text: "hi"}

	// A stream channel has no platform-side redelivery, so a message the
	// pipeline refuses is genuinely lost. Ending the connection makes that
	// visible instead of dropping it quietly.
	err := c.Run(context.Background(), binding, sink)
	if err == nil {
		t.Fatal("Run should fail when the sink rejects a message")
	}
}

func TestRunRequiresBindingAndSink(t *testing.T) {
	c := New(make(Feed))
	if err := c.Run(context.Background(), nil, &recordingSink{}); err == nil {
		t.Error("Run without a binding should fail")
	}
	if err := c.Run(context.Background(), &types.ChannelBinding{}, nil); err == nil {
		t.Error("Run without a sink should fail")
	}
}

func TestSendReplyRequiresALiveConnection(t *testing.T) {
	c := New(make(Feed))
	binding := &types.ChannelBinding{ChannelBindingID: "bind-1", TenantID: "tenant-demo"}

	// This is the failure the reversed reply path must not hide: a reply
	// handed to a replica that no longer holds the socket has to fail, so the
	// delivery is marked failed and retried rather than reported as sent.
	err := c.SendReply(context.Background(), binding, &types.StreamReply{Text: "hi"})
	if err == nil {
		t.Fatal("SendReply should fail with no live connection")
	}
}

func TestSendReplyRecordsDeliveredReplies(t *testing.T) {
	feed := make(Feed)
	c := New(feed)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	binding := &types.ChannelBinding{ChannelBindingID: "bind-1", TenantID: "tenant-demo"}

	go func() { _ = c.Run(ctx, binding, &recordingSink{}) }()

	deadline := time.Now().Add(time.Second)
	for !c.Live() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	want := &types.StreamReply{
		ChannelBindingID: "bind-1",
		TenantID:         "tenant-demo",
		CorrelationID:    "req-from-platform",
		Text:             "the answer",
	}
	if err := c.SendReply(ctx, binding, want); err != nil {
		t.Fatalf("SendReply: %v", err)
	}

	sent := c.Sent()
	if len(sent) != 1 {
		t.Fatalf("Sent() has %d replies, want 1", len(sent))
	}
	// The correlation token is what the platform matches the reply to. It
	// must arrive unmodified.
	if sent[0].CorrelationID != want.CorrelationID {
		t.Errorf("CorrelationID = %q, want %q", sent[0].CorrelationID, want.CorrelationID)
	}
	if sent[0].Text != want.Text {
		t.Errorf("Text = %q, want %q", sent[0].Text, want.Text)
	}
}

func TestRunCountIsObservable(t *testing.T) {
	feed := make(Feed)
	close(feed)
	c := New(feed)
	binding := &types.ChannelBinding{ChannelBindingID: "bind-1", TenantID: "tenant-demo"}

	// Run count is how the election test proves exactly one replica connected.
	if c.Runs() != 0 {
		t.Fatalf("Runs() = %d before any run, want 0", c.Runs())
	}
	if err := c.Run(context.Background(), binding, &recordingSink{}); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if c.Runs() != 1 {
		t.Fatalf("Runs() = %d after one run, want 1", c.Runs())
	}
}
