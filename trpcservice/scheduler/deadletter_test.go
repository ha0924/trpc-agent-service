package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

func TestAttemptCounterIsPerMessage(t *testing.T) {
	// The budget is per message, not per session: one poison message must not
	// consume the retry allowance of every later message in the same
	// conversation.
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	a, b := "req-"+uuid.NewString(), "req-"+uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, attemptKey(a), attemptKey(b)) })

	for want := 1; want <= 3; want++ {
		got, err := r.RecordAttempt(ctx, a)
		if err != nil {
			t.Fatalf("RecordAttempt: %v", err)
		}
		if got != want {
			t.Fatalf("attempt = %d, want %d", got, want)
		}
	}

	// The other message is untouched.
	got, err := r.RecordAttempt(ctx, b)
	if err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}
	if got != 1 {
		t.Errorf("second message inherited a count: %d", got)
	}
}

func TestClearAttemptsResetsBudget(t *testing.T) {
	// A conversation that recovers must not carry a stale budget into its
	// next failure, otherwise a single earlier hiccup would make the next
	// error terminal.
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	req := "req-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, attemptKey(req)) })

	for i := 0; i < 2; i++ {
		if _, err := r.RecordAttempt(ctx, req); err != nil {
			t.Fatalf("RecordAttempt: %v", err)
		}
	}
	if err := r.ClearAttempts(ctx, req); err != nil {
		t.Fatalf("ClearAttempts: %v", err)
	}

	got, err := r.RecordAttempt(ctx, req)
	if err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}
	if got != 1 {
		t.Errorf("counter not reset: %d", got)
	}
}

func TestDeadLetterPreservesTheReason(t *testing.T) {
	// A parked message is only useful with the reason attached; a bare count
	// tells nobody what to fix.
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, deadLetterKey(session)) })

	msg := &types.InboundMessage{
		RequestID: "req-1", Text: "poison", Channel: "mock",
		TenantID: "tenant-demo", ExternalEventID: "evt-1",
	}
	if err := r.PushDeadLetter(ctx, session, &DeadLetter{
		Message: msg, Attempts: 3, LastError: "tool exploded",
		FailedAt: time.Now(), WorkerID: "worker-1",
	}); err != nil {
		t.Fatalf("PushDeadLetter: %v", err)
	}

	got, err := r.ListDeadLetters(ctx, session, 10)
	if err != nil {
		t.Fatalf("ListDeadLetters: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d dead letters, want 1", len(got))
	}
	if got[0].LastError != "tool exploded" {
		t.Errorf("LastError = %q", got[0].LastError)
	}
	if got[0].Attempts != 3 {
		t.Errorf("Attempts = %d", got[0].Attempts)
	}
	if got[0].Message == nil || got[0].Message.Text != "poison" {
		t.Errorf("message not preserved: %+v", got[0].Message)
	}
	if got[0].WorkerID != "worker-1" {
		t.Errorf("WorkerID = %q", got[0].WorkerID)
	}
}

func TestDeadLetterRetentionIsBounded(t *testing.T) {
	// A session that fails repeatedly must not grow this list without limit;
	// the recent failures are the diagnostic ones.
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, deadLetterKey(session)) })

	for i := 0; i < 150; i++ {
		if err := r.PushDeadLetter(ctx, session, &DeadLetter{
			Message:  &types.InboundMessage{RequestID: "req", Text: "x"},
			Attempts: 3, FailedAt: time.Now(),
		}); err != nil {
			t.Fatalf("PushDeadLetter %d: %v", i, err)
		}
	}

	n, err := r.DeadLetterCount(ctx, session)
	if err != nil {
		t.Fatalf("DeadLetterCount: %v", err)
	}
	if n > 100 {
		t.Errorf("retained %d dead letters, want at most 100", n)
	}
}

func TestReplayReturnsMessageToMailboxAndResetsBudget(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	session := "sess-" + uuid.NewString()
	req := "req-" + uuid.NewString()
	t.Cleanup(func() {
		r.Client().Del(ctx, deadLetterKey(session), mailboxKey(session), attemptKey(req))
	})

	// Exhaust the budget, then park the message.
	for i := 0; i < 3; i++ {
		r.RecordAttempt(ctx, req)
	}
	msg := &types.InboundMessage{RequestID: req, Text: "retry me", Channel: "mock"}
	if err := r.PushDeadLetter(ctx, session, &DeadLetter{
		Message: msg, Attempts: 3, LastError: "transient", FailedAt: time.Now(),
	}); err != nil {
		t.Fatalf("PushDeadLetter: %v", err)
	}

	replayed, err := r.ReplayDeadLetter(ctx, session)
	if err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}
	if replayed == nil || replayed.Text != "retry me" {
		t.Fatalf("replayed message wrong: %+v", replayed)
	}

	// The budget has to be reset, otherwise the replayed message immediately
	// exceeds it again and lands straight back in the dead letter — a replay
	// that cannot possibly succeed.
	n, err := r.RecordAttempt(ctx, req)
	if err != nil {
		t.Fatalf("RecordAttempt: %v", err)
	}
	if n != 1 {
		t.Errorf("budget not reset on replay: attempt = %d", n)
	}

	// It must be readable from the mailbox as a plain message, not as a
	// dead-letter envelope, or the drain would fail to unmarshal it.
	popped, err := r.Pop(ctx, session)
	if err != nil {
		t.Fatalf("Pop: %v", err)
	}
	if popped == nil || popped.Text != "retry me" {
		t.Fatalf("mailbox does not hold the replayed message: %+v", popped)
	}

	// And it is gone from the dead letter list.
	if count, _ := r.DeadLetterCount(ctx, session); count != 0 {
		t.Errorf("dead letter still present after replay: %d", count)
	}
}

func TestReplayOnEmptyIsNotAnError(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	got, err := r.ReplayDeadLetter(ctx, "sess-"+uuid.NewString())
	if err != nil {
		t.Fatalf("replay on empty should not error: %v", err)
	}
	if got != nil {
		t.Errorf("expected nil message, got %+v", got)
	}
}

func TestReplayGoesToTheTail(t *testing.T) {
	// A replay is a deliberate operator action after the cause was fixed.
	// Jumping ahead of messages the user has sent since would reorder the
	// conversation.
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()

	session := "sess-" + uuid.NewString()
	t.Cleanup(func() {
		r.Client().Del(ctx, deadLetterKey(session), mailboxKey(session))
	})

	// A message the user sent while the failure was being investigated.
	if err := r.Push(ctx, session, &types.InboundMessage{RequestID: "newer", Text: "newer"}); err != nil {
		t.Fatalf("Push: %v", err)
	}
	if err := r.PushDeadLetter(ctx, session, &DeadLetter{
		Message: &types.InboundMessage{RequestID: "older", Text: "older"}, Attempts: 3,
	}); err != nil {
		t.Fatalf("PushDeadLetter: %v", err)
	}
	if _, err := r.ReplayDeadLetter(ctx, session); err != nil {
		t.Fatalf("ReplayDeadLetter: %v", err)
	}

	first, _ := r.Pop(ctx, session)
	if first == nil || first.Text != "newer" {
		t.Fatalf("replay jumped the queue: first popped = %+v", first)
	}
	second, _ := r.Pop(ctx, session)
	if second == nil || second.Text != "older" {
		t.Fatalf("replayed message not at the tail: %+v", second)
	}
}
