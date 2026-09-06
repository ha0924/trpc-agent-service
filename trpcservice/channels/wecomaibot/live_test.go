package wecomaibot

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Live integration test against WeCom's real endpoint.
//
// Skipped unless credentials are supplied, following the same convention as
// the scheduler's TEST_REDIS_ADDR tests:
//
//	WECOM_AIBOT_BOT_ID=aib... WECOM_AIBOT_SECRET=... \
//	  go test ./trpcservice/channels/wecomaibot/ -run TestLive -v
//
// It exists because the protocol tests in wecomaibot_test.go verify this
// implementation against a server built from the documentation — which means
// they verify my reading of the docs, not the docs themselves. Anything the
// documentation states imprecisely would be wrong in both the code and the
// fake server, and pass. Only the real endpoint settles it.
//
// Credentials come from the environment, never a file: a secret in a test
// fixture is a secret in the repository.
func liveCredentials(t *testing.T) SecretResolver {
	t.Helper()

	botID := os.Getenv("WECOM_AIBOT_BOT_ID")
	secret := os.Getenv("WECOM_AIBOT_SECRET")
	if botID == "" || secret == "" {
		t.Skip("WECOM_AIBOT_BOT_ID / WECOM_AIBOT_SECRET not set; skipping live test")
	}

	blob, err := json.Marshal(Credentials{BotID: botID, Secret: secret})
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	return func(ref string) (string, error) { return string(blob), nil }
}

// TestLiveSubscribe checks the one thing no fake can: that WeCom accepts our
// subscribe frame.
//
// A successful subscribe proves the endpoint, the frame shape, the field
// names and the credential semantics are all right. Every later behaviour
// depends on it, so a failure here localises the problem immediately.
func TestLiveSubscribe(t *testing.T) {
	secrets := liveCredentials(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := New(secrets, logger)

	binding := &types.ChannelBinding{
		ChannelBindingID: "live-probe",
		TenantID:         "tenant-live",
		AgentAppID:       "assistant",
		Channel:          Name,
		SecretRef:        "secret://live",
		Status:           types.StatusActive,
	}

	// Long enough to complete the handshake, short enough that a hung dial
	// fails the test rather than the suite timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sink := &liveSink{}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, binding, sink) }()

	// Run logs "wecom aibot subscribed" once the handshake is accepted. A
	// rejection returns immediately, so waiting briefly and then checking
	// whether Run is still alive distinguishes the two without needing a
	// hook into the channel's internals.
	select {
	case err := <-done:
		// Returning this fast means the handshake failed: a healthy
		// connection blocks in the read loop waiting for pushes.
		t.Fatalf("Run returned during handshake, subscribe likely rejected: %v", err)
	case <-time.After(8 * time.Second):
		// Still connected. That is the pass condition.
	}

	t.Log("subscribe accepted; connection is live")

	cancel()
	select {
	case err := <-done:
		t.Logf("connection closed after cancel: %v", err)
	case <-time.After(10 * time.Second):
		// This is the failure mode found earlier: Run not returning after
		// its context ends means something is not wired to shutdown.
		t.Fatal("Run did not return within 10s of cancellation")
	}
}

// TestLiveReceive holds a connection open and reports whatever arrives.
//
// Run this, then send the bot a message in WeCom. It prints the decoded
// message so the fields the documentation describes loosely — the userid
// form when the creator is not a super admin, whether chatid appears for a
// direct chat — can be read off a real payload instead of guessed at.
func TestLiveReceive(t *testing.T) {
	if os.Getenv("WECOM_AIBOT_INTERACTIVE") == "" {
		t.Skip("WECOM_AIBOT_INTERACTIVE not set; skipping interactive receive test")
	}
	secrets := liveCredentials(t)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
	c := New(secrets, logger)

	binding := &types.ChannelBinding{
		ChannelBindingID: "live-probe",
		TenantID:         "tenant-live",
		AgentAppID:       "assistant",
		Channel:          Name,
		SecretRef:        "secret://live",
		Status:           types.StatusActive,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	// Echo whatever arrives, which also exercises the reply path: a reply
	// that WeCom rejects shows up as an errcode in the logs.
	sink := &liveSink{echo: c, binding: binding, t: t}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, binding, sink) }()

	t.Log("connected — send the bot a message in WeCom now (waiting up to 3 minutes)")

	deadline := time.Now().Add(3 * time.Minute)
	for time.Now().Before(deadline) {
		if len(sink.messages()) > 0 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("connection ended while waiting: %v", err)
		case <-time.After(500 * time.Millisecond):
		}
	}

	msgs := sink.messages()
	if len(msgs) == 0 {
		t.Fatal("no message arrived within the window")
	}

	for i, m := range msgs {
		// Printed rather than asserted: the point is to observe the real
		// field values, several of which the documentation only describes in
		// prose.
		t.Logf("message %d: user=%q scope=%q scope_key=%q event_id=%q correlation=%q text=%q",
			i, m.ExternalUserID, m.Scope, m.ScopeKey,
			m.ExternalEventID, m.CorrelationID, m.Text)
	}

	cancel()
	<-done
}

// liveSink records inbound messages and optionally echoes a reply.
type liveSink struct {
	mu  sync.Mutex
	got []*types.InboundMessage

	// echo, when set, sends a reply back — exercising the outbound half
	// against the real platform rather than only the inbound one.
	echo    *Channel
	binding *types.ChannelBinding
	t       *testing.T
}

func (s *liveSink) Accept(ctx context.Context, msg *types.InboundMessage) (types.AckInfo, error) {
	s.mu.Lock()
	s.got = append(s.got, msg)
	s.mu.Unlock()

	if s.echo != nil {
		target := msg.ScopeKey
		reply := &types.StreamReply{
			Channel:          Name,
			ChannelBindingID: s.binding.ChannelBindingID,
			TenantID:         s.binding.TenantID,
			Target:           target,
			Scope:            msg.Scope,
			CorrelationID:    msg.CorrelationID,
			ExternalEventID:  msg.ExternalEventID,
			Text:             "收到：" + msg.Text,
		}
		if err := s.echo.SendReply(ctx, s.binding, reply); err != nil {
			s.t.Errorf("SendReply failed: %v", err)
		} else {
			s.t.Log("reply sent")
		}
	}

	return types.AckInfo{RequestID: msg.RequestID, SessionID: "live-session"}, nil
}

func (s *liveSink) messages() []*types.InboundMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*types.InboundMessage, len(s.got))
	copy(out, s.got)
	return out
}
