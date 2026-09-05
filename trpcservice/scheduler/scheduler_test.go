package scheduler

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Run against a real Redis; the lease semantics depend on SETNX and Lua
// atomicity, which a fake cannot reproduce faithfully.
//
//	TEST_REDIS_ADDR=127.0.0.1:6379 go test ./trpcservice/scheduler/
func testRedis(t *testing.T, ttl time.Duration) *Redis {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping scheduler integration tests")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}
	queueKey := "test:queue:" + uuid.NewString()
	t.Cleanup(func() {
		client.Del(context.Background(), queueKey)
		client.Close()
	})
	return NewWithClient(client, queueKey, ttl)
}

func TestLeaseAdmitsOneOwner(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, leaseKey(session)) })

	got, err := r.Acquire(ctx, session, "worker-a")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if !got {
		t.Fatal("first acquire should succeed")
	}

	// The second worker losing the race is the normal case under
	// at-least-once delivery, and must be reported without an error.
	got, err = r.Acquire(ctx, session, "worker-b")
	if err != nil {
		t.Fatalf("second Acquire returned error: %v", err)
	}
	if got {
		t.Fatal("second acquire should fail while the lease is held")
	}
}

func TestOnlyOwnerCanRenew(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, leaseKey(session)) })

	if _, err := r.Acquire(ctx, session, "worker-a"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	ok, err := r.Renew(ctx, session, "worker-a")
	if err != nil {
		t.Fatalf("Renew: %v", err)
	}
	if !ok {
		t.Error("owner should be able to renew")
	}

	// A worker that lost its lease must not be able to extend it — otherwise
	// it could keep writing to a session another worker now owns.
	ok, err = r.Renew(ctx, session, "worker-b")
	if err != nil {
		t.Fatalf("Renew by non-owner: %v", err)
	}
	if ok {
		t.Error("non-owner must not be able to renew")
	}
}

func TestOnlyOwnerCanRelease(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, leaseKey(session)) })

	if _, err := r.Acquire(ctx, session, "worker-a"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// A slow worker must not be able to drop a lease that has been taken
	// over, which would let a third worker in alongside the current owner.
	if err := r.Release(ctx, session, "worker-b"); err != nil {
		t.Fatalf("Release by non-owner: %v", err)
	}
	owner, err := r.LeaseOwner(ctx, session)
	if err != nil {
		t.Fatalf("LeaseOwner: %v", err)
	}
	if owner != "worker-a" {
		t.Fatalf("lease owner = %q, want worker-a", owner)
	}

	if err := r.Release(ctx, session, "worker-a"); err != nil {
		t.Fatalf("Release by owner: %v", err)
	}
	if owner, _ := r.LeaseOwner(ctx, session); owner != "" {
		t.Errorf("lease should be gone, owner = %q", owner)
	}
}

func TestLeaseExpiresSoAFailedWorkerDoesNotBlockForever(t *testing.T) {
	// The TTL is the failure-recovery time: a worker that dies holding a
	// lease blocks its session only until the lease expires.
	r := testRedis(t, 300*time.Millisecond)
	ctx := context.Background()
	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, leaseKey(session)) })

	if _, err := r.Acquire(ctx, session, "worker-dead"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	time.Sleep(500 * time.Millisecond)

	got, err := r.Acquire(ctx, session, "worker-live")
	if err != nil {
		t.Fatalf("Acquire after expiry: %v", err)
	}
	if !got {
		t.Fatal("another worker should take over after the lease expires")
	}
}

func TestMailboxPreservesArrivalOrder(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx := context.Background()
	session := "sess-" + uuid.NewString()
	t.Cleanup(func() { r.Client().Del(ctx, mailboxKey(session)) })

	for _, text := range []string{"first", "second", "third"} {
		if err := r.Push(ctx, session, &types.InboundMessage{Text: text}); err != nil {
			t.Fatalf("Push %s: %v", text, err)
		}
	}

	if n, err := r.Len(ctx, session); err != nil || n != 3 {
		t.Fatalf("Len = %d, err = %v", n, err)
	}

	// Ordering is the mailbox's job, not the queue's; this is the guarantee
	// the whole lease design exists to protect.
	for _, want := range []string{"first", "second", "third"} {
		msg, err := r.Pop(ctx, session)
		if err != nil {
			t.Fatalf("Pop: %v", err)
		}
		if msg == nil {
			t.Fatalf("mailbox drained early, wanted %q", want)
		}
		if msg.Text != want {
			t.Fatalf("got %q, want %q", msg.Text, want)
		}
	}

	// Draining to empty must be a nil message, not an error: it is the normal
	// signal to release the lease.
	msg, err := r.Pop(ctx, session)
	if err != nil {
		t.Fatalf("Pop on empty mailbox: %v", err)
	}
	if msg != nil {
		t.Errorf("expected nil on empty mailbox, got %+v", msg)
	}
}

func TestPublishSubscribeRoundTrip(t *testing.T) {
	r := testRedis(t, 5*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hints, err := r.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	want := types.SessionHint{
		TenantID: "tenant-demo", AgentAppID: "assistant",
		AgentVersion: "v1", SessionID: "sess-1", TraceID: "trace-1",
	}
	if err := r.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-hints:
		if got != want {
			t.Errorf("got %+v, want %+v", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for hint")
	}
}

func TestSubscribeStopsOnCancel(t *testing.T) {
	// A subscriber that ignores cancellation would leak a goroutine and hold
	// a Redis connection through shutdown.
	r := testRedis(t, 5*time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	hints, err := r.Subscribe(ctx)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()

	select {
	case _, open := <-hints:
		if open {
			// Draining one buffered hint before close is acceptable.
			select {
			case _, open := <-hints:
				if open {
					t.Error("channel should close after cancel")
				}
			case <-time.After(3 * time.Second):
				t.Error("channel did not close after cancel")
			}
		}
	case <-time.After(3 * time.Second):
		t.Error("channel did not close after cancel")
	}
}
