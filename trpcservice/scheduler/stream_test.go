package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// The connection lease is what stops two Gateway replicas from displacing
// each other on a platform that allows a bot one socket. Its correctness is
// the same class of property as the session lease's, so it gets the same
// treatment: real Redis, and the mutual-exclusion case tested explicitly.

func TestConnectionLeaseAdmitsOneHolder(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	binding := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, connLeaseKey(binding)) })

	got, err := r.AcquireConnection(ctx, binding, "gateway-a")
	if err != nil {
		t.Fatalf("AcquireConnection: %v", err)
	}
	if !got {
		t.Fatal("first acquire should win")
	}

	// Standing by is the expected state for every replica but one, so it is
	// reported as false without an error.
	got, err = r.AcquireConnection(ctx, binding, "gateway-b")
	if err != nil {
		t.Fatalf("second AcquireConnection returned error: %v", err)
	}
	if got {
		t.Fatal("second acquire should lose while the lease is held")
	}
}

func TestOnlyHolderCanRenewConnection(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	binding := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, connLeaseKey(binding)) })

	if _, err := r.AcquireConnection(ctx, binding, "gateway-a"); err != nil {
		t.Fatalf("AcquireConnection: %v", err)
	}

	ok, err := r.RenewConnection(ctx, binding, "gateway-b")
	if err != nil {
		t.Fatalf("RenewConnection: %v", err)
	}
	if ok {
		// If a non-holder could renew, it would keep a connection the
		// platform has already replaced, and both replicas would process the
		// same messages.
		t.Fatal("a non-holder must not be able to renew")
	}

	ok, err = r.RenewConnection(ctx, binding, "gateway-a")
	if err != nil {
		t.Fatalf("RenewConnection by holder: %v", err)
	}
	if !ok {
		t.Fatal("the holder should be able to renew")
	}
}

func TestReleaseConnectionIsOwnerScoped(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	binding := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, connLeaseKey(binding)) })

	if _, err := r.AcquireConnection(ctx, binding, "gateway-a"); err != nil {
		t.Fatalf("AcquireConnection: %v", err)
	}

	// A replica that lost its lease and only noticed later must not be able
	// to evict the replica that legitimately took over.
	if err := r.ReleaseConnection(ctx, binding, "gateway-b"); err != nil {
		t.Fatalf("ReleaseConnection by non-owner: %v", err)
	}
	owner, err := r.ConnectionOwner(ctx, binding)
	if err != nil {
		t.Fatalf("ConnectionOwner: %v", err)
	}
	if owner != "gateway-a" {
		t.Fatalf("owner changed after a non-owner release: got %q", owner)
	}

	if err := r.ReleaseConnection(ctx, binding, "gateway-a"); err != nil {
		t.Fatalf("ReleaseConnection by owner: %v", err)
	}
	owner, err = r.ConnectionOwner(ctx, binding)
	if err != nil {
		t.Fatalf("ConnectionOwner after release: %v", err)
	}
	if owner != "" {
		t.Fatalf("lease should be free after the owner released it: got %q", owner)
	}
}

func TestConnectionLeaseIsPerBinding(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	a := "bind-" + uuid.NewString()
	b := "bind-" + uuid.NewString()
	t.Cleanup(func() {
		r.Client().Del(ctx, connLeaseKey(a), connLeaseKey(b))
	})

	// Two bots must not contend: holding one binding's connection says
	// nothing about another's, including across tenants.
	if got, err := r.AcquireConnection(ctx, a, "gateway-a"); err != nil || !got {
		t.Fatalf("acquire a: got=%v err=%v", got, err)
	}
	if got, err := r.AcquireConnection(ctx, b, "gateway-b"); err != nil || !got {
		t.Fatalf("acquire b should be independent: got=%v err=%v", got, err)
	}
}

func TestOutboxPreservesOrderPerBinding(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	binding := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, outboxKey(binding)) })

	// A long reply is split into several messages, so the order they leave in
	// is the order the user reads. Reversal would be visible.
	for _, text := range []string{"first", "second", "third"} {
		if err := r.PushReply(ctx, &types.StreamReply{
			ChannelBindingID: binding,
			TenantID:         "tenant-demo",
			Text:             text,
		}); err != nil {
			t.Fatalf("PushReply(%s): %v", text, err)
		}
	}

	n, err := r.ReplyLen(ctx, binding)
	if err != nil {
		t.Fatalf("ReplyLen: %v", err)
	}
	if n != 3 {
		t.Fatalf("ReplyLen = %d, want 3", n)
	}

	for _, want := range []string{"first", "second", "third"} {
		got, err := r.PopReply(ctx, binding)
		if err != nil {
			t.Fatalf("PopReply: %v", err)
		}
		if got == nil {
			t.Fatalf("PopReply returned nil while expecting %q", want)
		}
		if got.Text != want {
			t.Fatalf("PopReply = %q, want %q", got.Text, want)
		}
	}

	// Draining to empty must be a nil result rather than an error: it is the
	// normal idle state of the sender loop, which polls continuously.
	got, err := r.PopReply(ctx, binding)
	if err != nil {
		t.Fatalf("PopReply on empty outbox: %v", err)
	}
	if got != nil {
		t.Fatalf("PopReply on empty outbox = %+v, want nil", got)
	}
}

func TestOutboxIsolatesBindings(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	a := "bind-" + uuid.NewString()
	b := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, outboxKey(a), outboxKey(b)) })

	// Two tenants' bots may be connected from different replicas. A reply
	// leaking into the wrong outbox would be delivered to the wrong tenant's
	// users — the worst failure this system can have.
	if err := r.PushReply(ctx, &types.StreamReply{
		ChannelBindingID: a, TenantID: "tenant-demo", Text: "for-demo",
	}); err != nil {
		t.Fatalf("PushReply a: %v", err)
	}

	got, err := r.PopReply(ctx, b)
	if err != nil {
		t.Fatalf("PopReply b: %v", err)
	}
	if got != nil {
		t.Fatalf("binding b saw binding a's reply: %+v", got)
	}

	got, err = r.PopReply(ctx, a)
	if err != nil {
		t.Fatalf("PopReply a: %v", err)
	}
	if got == nil || got.Text != "for-demo" {
		t.Fatalf("PopReply a = %+v, want the pushed reply", got)
	}
}

func TestPushReplyRejectsMissingBinding(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	// Without a binding id there is no outbox to route to. Failing loudly
	// beats writing to an empty key, where the reply would be invisible
	// rather than merely late.
	if err := r.PushReply(ctx, &types.StreamReply{TenantID: "tenant-demo", Text: "x"}); err == nil {
		t.Fatal("PushReply with no binding id should fail")
	}
	if err := r.PushReply(ctx, nil); err == nil {
		t.Fatal("PushReply(nil) should fail")
	}
}

func TestStreamReplyRoundTripsCorrelation(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	binding := "bind-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, outboxKey(binding)) })

	// The correlation token is the platform's own, and a reply that loses it
	// cannot be matched to the message it answers — WeCom would reject it.
	// It has to survive the JSON round trip through Redis intact.
	want := &types.StreamReply{
		Channel:          "wecom_aibot",
		ChannelBindingID: binding,
		TenantID:         "tenant-demo",
		Target:           "user-1",
		Scope:            types.ScopeSingle,
		CorrelationID:    "req-from-platform-42",
		ExternalEventID:  "msg-99",
		Text:             "hello",
		SessionID:        "sess-1",
		RequestID:        "req-1",
		TraceID:          "trace-1",
		TraceContext:     map[string]string{"traceparent": "00-abc-def-01"},
	}
	if err := r.PushReply(ctx, want); err != nil {
		t.Fatalf("PushReply: %v", err)
	}

	got, err := r.PopReply(ctx, binding)
	if err != nil {
		t.Fatalf("PopReply: %v", err)
	}
	if got == nil {
		t.Fatal("PopReply returned nil")
	}
	if got.CorrelationID != want.CorrelationID {
		t.Errorf("CorrelationID = %q, want %q", got.CorrelationID, want.CorrelationID)
	}
	if got.ExternalEventID != want.ExternalEventID {
		t.Errorf("ExternalEventID = %q, want %q", got.ExternalEventID, want.ExternalEventID)
	}
	if got.TraceContext["traceparent"] != want.TraceContext["traceparent"] {
		t.Errorf("traceparent = %q, want %q",
			got.TraceContext["traceparent"], want.TraceContext["traceparent"])
	}
}
