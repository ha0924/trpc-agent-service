package telegram

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Tested against a real HTTP server rather than a stubbed transport, because
// the parts most likely to be wrong are sequencing properties a stub would
// let pass: that the cursor advances only after a message is accepted, that a
// refused message is re-delivered, that the token never reaches an error
// string.

// fakeAPI stands in for api.telegram.org.
type fakeAPI struct {
	srv *httptest.Server

	mu sync.Mutex
	// calls records method names in order.
	calls []string
	// offsets records the offset each getUpdates asked for, which is how the
	// acknowledgement cursor is observed.
	offsets []int64
	// batches are returned one per getUpdates call, then empty.
	batches [][]update
	sent    []map[string]any
	// unauthorized makes every call fail as a bad token would.
	unauthorized bool
}

func newFakeAPI(t *testing.T) *fakeAPI {
	t.Helper()
	f := &fakeAPI{}

	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Path is /bot<token>/<method>; the token must be present but is not
		// asserted on beyond that.
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) != 2 || !strings.HasPrefix(parts[0], "bot") {
			http.Error(w, "bad path", http.StatusNotFound)
			return
		}
		method := parts[1]

		var body map[string]any
		if raw, _ := io.ReadAll(r.Body); len(raw) > 0 {
			json.Unmarshal(raw, &body)
		}

		f.mu.Lock()
		f.calls = append(f.calls, method)
		unauth := f.unauthorized
		f.mu.Unlock()

		if unauth {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(apiResponse{
				OK: false, ErrorCode: 401, Description: "Unauthorized",
			})
			return
		}

		switch method {
		case "getMe":
			f.reply(w, map[string]any{"id": 42, "username": "test_bot"})

		case "getUpdates":
			f.mu.Lock()
			if off, ok := body["offset"]; ok {
				switch v := off.(type) {
				case float64:
					f.offsets = append(f.offsets, int64(v))
				}
			} else {
				f.offsets = append(f.offsets, 0)
			}
			var batch []update
			if len(f.batches) > 0 {
				batch, f.batches = f.batches[0], f.batches[1:]
			}
			f.mu.Unlock()
			f.reply(w, batch)

		case "sendMessage":
			f.mu.Lock()
			f.sent = append(f.sent, body)
			f.mu.Unlock()
			f.reply(w, map[string]any{"message_id": 99})

		default:
			f.reply(w, map[string]any{})
		}
	}))

	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAPI) reply(w http.ResponseWriter, result any) {
	raw, _ := json.Marshal(result)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(apiResponse{OK: true, Result: raw})
}

func (f *fakeAPI) queue(batch []update) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.batches = append(f.batches, batch)
}

func (f *fakeAPI) askedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]int64, len(f.offsets))
	copy(out, f.offsets)
	return out
}

func (f *fakeAPI) sentMessages() []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]map[string]any, len(f.sent))
	copy(out, f.sent)
	return out
}

// collectSink records what the channel hands to the pipeline.
type collectSink struct {
	mu  sync.Mutex
	got []*types.InboundMessage
	err error
}

func (s *collectSink) Accept(ctx context.Context, msg *types.InboundMessage) (types.AckInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return types.AckInfo{}, s.err
	}
	s.got = append(s.got, msg)
	return types.AckInfo{RequestID: "req-1"}, nil
}

func (s *collectSink) messages() []*types.InboundMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*types.InboundMessage, len(s.got))
	copy(out, s.got)
	return out
}

func testChannel(t *testing.T, f *fakeAPI) *Channel {
	t.Helper()
	return New(
		func(ref string) (string, error) { return "123456:test-token", nil },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithAPIBase(f.srv.URL),
		// Short client timeout so tests do not wait out the long poll.
		WithHTTPClient(&http.Client{Timeout: 3 * time.Second}),
	)
}

func testBinding() *types.ChannelBinding {
	return &types.ChannelBinding{
		ChannelBindingID: "cb-tg-1",
		TenantID:         "tenant-test",
		AgentAppID:       "assistant",
		Channel:          Name,
		SecretRef:        "secret://telegram",
		Status:           types.StatusActive,
	}
}

func waitFor(cond func() bool, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func TestCapabilitiesPairStreamInWithDirectOut(t *testing.T) {
	c := New(nil, nil)
	caps := c.Capabilities()

	// This pairing is why inbound and outbound mode had to be split. A run
	// loop is needed (we dial out), but replies must not go through the
	// outbox — they are ordinary HTTPS calls.
	if !caps.StreamCapable() {
		t.Error("telegram long-polls, so it needs a Run loop")
	}
	if caps.RepliesViaHolder() {
		t.Error("telegram replies directly; routing via the holder would add a hop and an election for nothing")
	}
	if caps.MaxTextLength != maxTextLength {
		t.Errorf("MaxTextLength = %d, want %d", caps.MaxTextLength, maxTextLength)
	}
	if !caps.SupportsEdit {
		t.Error("telegram can edit sent messages, unlike either WeCom form")
	}
}

func TestParseCredentialsAcceptsBareToken(t *testing.T) {
	// Telegram hands out a plain string. Requiring JSON invites a paste
	// mistake that surfaces much later as an auth failure.
	c, err := ParseCredentials("123456:ABC-DEF")
	if err != nil {
		t.Fatalf("bare token: %v", err)
	}
	if c.BotToken != "123456:ABC-DEF" {
		t.Errorf("BotToken = %q", c.BotToken)
	}

	c, err = ParseCredentials(`{"bot_token":"789:XYZ"}`)
	if err != nil {
		t.Fatalf("json form: %v", err)
	}
	if c.BotToken != "789:XYZ" {
		t.Errorf("BotToken = %q", c.BotToken)
	}

	for _, bad := range []string{"", "   ", `{"bot_token":""}`, `{oops`} {
		if _, err := ParseCredentials(bad); err == nil {
			t.Errorf("ParseCredentials(%q) should fail", bad)
		}
	}
}

func TestBadTokenFailsBeforePolling(t *testing.T) {
	f := newFakeAPI(t)
	f.mu.Lock()
	f.unauthorized = true
	f.mu.Unlock()

	c := testChannel(t, f)

	// getMe runs first so a wrong token fails immediately, rather than as an
	// endless stream of poll errors that look like a network problem.
	err := c.Run(context.Background(), testBinding(), &collectSink{})
	if err == nil {
		t.Fatal("Run should fail when the token is rejected")
	}
	if !strings.Contains(err.Error(), "verify token") {
		t.Errorf("error = %v, want it to mention token verification", err)
	}
}

func TestPrivateMessageBecomesSingleScope(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{{
		UpdateID: 1001,
		Message: &tgMessage{
			MessageID: 7,
			From:      &tgUser{ID: 555, Username: "alice"},
			Chat:      tgChat{ID: 555, Type: "private"},
			Date:      time.Now().Unix(),
			Text:      "hello bot",
		},
	}})

	c := testChannel(t, f)
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	if !waitFor(func() bool { return len(sink.messages()) > 0 }, 3*time.Second) {
		t.Fatal("no message reached the pipeline")
	}
	got := sink.messages()[0]

	if got.Text != "hello bot" {
		t.Errorf("Text = %q", got.Text)
	}
	// update_id, not message_id: message_id is unique per chat, so two chats
	// could collide on it.
	if got.ExternalEventID != "1001" {
		t.Errorf("ExternalEventID = %q, want the update_id 1001", got.ExternalEventID)
	}
	if got.Scope != types.ScopeSingle {
		t.Errorf("Scope = %q, want single", got.Scope)
	}
	if got.ScopeKey != "555" {
		t.Errorf("ScopeKey = %q, want the chat id", got.ScopeKey)
	}
}

func TestGroupMessageBecomesGroupScope(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{{
		UpdateID: 2002,
		Message: &tgMessage{
			From: &tgUser{ID: 555},
			// Group chat ids are negative in Telegram — a quirk worth keeping
			// visible rather than normalising away.
			Chat: tgChat{ID: -100200, Type: "supergroup", Title: "team"},
			Date: time.Now().Unix(),
			Text: "@bot status?",
		},
	}})

	c := testChannel(t, f)
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	if !waitFor(func() bool { return len(sink.messages()) > 0 }, 3*time.Second) {
		t.Fatal("no message reached the pipeline")
	}
	got := sink.messages()[0]

	// Group and direct conversations must land on different sessions, or one
	// member's turn would join everyone else's private history.
	if got.Scope != types.ScopeGroup {
		t.Errorf("Scope = %q, want group", got.Scope)
	}
	if got.ScopeKey != "-100200" {
		t.Errorf("ScopeKey = %q, want the negative group id", got.ScopeKey)
	}
	if got.ExternalUserID != "555" {
		t.Errorf("ExternalUserID = %q, want the speaker", got.ExternalUserID)
	}
}

func TestCursorAdvancesOnlyAfterAcceptance(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{{
		UpdateID: 3003,
		Message: &tgMessage{
			From: &tgUser{ID: 1}, Chat: tgChat{ID: 1, Type: "private"},
			Date: time.Now().Unix(), Text: "hi",
		},
	}})

	c := testChannel(t, f)
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	// The cursor is the acknowledgement: asking for offset N+1 is what tells
	// Telegram N is done. So the next poll must ask for 3004.
	if !waitFor(func() bool {
		for _, o := range f.askedOffsets() {
			if o == 3004 {
				return true
			}
		}
		return false
	}, 3*time.Second) {
		t.Fatalf("cursor never advanced past the accepted update; offsets=%v",
			f.askedOffsets())
	}
}

func TestRefusedMessageDoesNotAdvanceTheCursor(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{{
		UpdateID: 4004,
		Message: &tgMessage{
			From: &tgUser{ID: 1}, Chat: tgChat{ID: 1, Type: "private"},
			Date: time.Now().Unix(), Text: "hi",
		},
	}})

	c := testChannel(t, f)
	// The pipeline refuses, e.g. the idempotency write failed.
	sink := &collectSink{err: errAccept}

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), testBinding(), sink) }()

	select {
	case err := <-done:
		// Returning without advancing means Telegram re-delivers the same
		// update after the supervisor reconnects. Advancing first would drop
		// a message the pipeline never took.
		if err == nil {
			t.Fatal("Run should fail when the pipeline refuses a message")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after a refused message")
	}

	for _, o := range f.askedOffsets() {
		if o >= 4005 {
			t.Fatalf("cursor advanced past a refused message: offsets=%v", f.askedOffsets())
		}
	}
}

func TestBotMessagesAndEditsAreIgnored(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{
		{
			UpdateID: 5005,
			Message: &tgMessage{
				// Bots answering bots is an easy infinite loop.
				From: &tgUser{ID: 9, IsBot: true},
				Chat: tgChat{ID: 9, Type: "private"},
				Date: time.Now().Unix(), Text: "I am a bot",
			},
		},
		{
			UpdateID: 5006,
			// An edit must not re-run the agent: a user fixing a typo would
			// otherwise repeat every tool side effect.
			Edited: &tgMessage{
				From: &tgUser{ID: 1}, Chat: tgChat{ID: 1, Type: "private"},
				Date: time.Now().Unix(), Text: "fixed typo",
			},
		},
		{
			UpdateID: 5007,
			Message: &tgMessage{
				From: &tgUser{ID: 1}, Chat: tgChat{ID: 1, Type: "private"},
				Date: time.Now().Unix(), Text: "real question",
			},
		},
	})

	c := testChannel(t, f)
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	if !waitFor(func() bool { return len(sink.messages()) > 0 }, 3*time.Second) {
		t.Fatal("the real message never arrived")
	}
	msgs := sink.messages()
	if len(msgs) != 1 {
		t.Fatalf("pipeline saw %d messages, want only the non-bot, non-edit one", len(msgs))
	}
	if msgs[0].Text != "real question" {
		t.Errorf("Text = %q, want the real question", msgs[0].Text)
	}
}

func TestCaptionCountsAsText(t *testing.T) {
	f := newFakeAPI(t)
	f.queue([]update{{
		UpdateID: 6006,
		Message: &tgMessage{
			From: &tgUser{ID: 1}, Chat: tgChat{ID: 1, Type: "private"},
			Date: time.Now().Unix(),
			// An image with a caption still carries a question worth
			// answering, even though media itself is out of scope.
			Caption: "what is in this picture?",
		},
	}})

	c := testChannel(t, f)
	sink := &collectSink{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	if !waitFor(func() bool { return len(sink.messages()) > 0 }, 3*time.Second) {
		t.Fatal("a captioned message should reach the pipeline")
	}
	if got := sink.messages()[0].Text; got != "what is in this picture?" {
		t.Errorf("Text = %q, want the caption", got)
	}
}

func TestSendPostsToTheChat(t *testing.T) {
	f := newFakeAPI(t)
	c := testChannel(t, f)

	err := c.Send(context.Background(), "555",
		types.NewTextReply("the answer"), testBinding())
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	sent := f.sentMessages()
	if len(sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sent))
	}
	if sent[0]["chat_id"] != "555" {
		t.Errorf("chat_id = %v, want 555", sent[0]["chat_id"])
	}
	if sent[0]["text"] != "the answer" {
		t.Errorf("text = %v", sent[0]["text"])
	}
}

func TestSendRequiresATarget(t *testing.T) {
	f := newFakeAPI(t)
	c := testChannel(t, f)

	if err := c.Send(context.Background(), "",
		types.NewTextReply("x"), testBinding()); err == nil {
		t.Fatal("Send with no chat id should fail")
	}
}

func TestErrorsNeverLeakTheToken(t *testing.T) {
	f := newFakeAPI(t)
	f.mu.Lock()
	f.unauthorized = true
	f.mu.Unlock()

	const token = "123456:super-secret-token"
	c := New(
		func(ref string) (string, error) { return token, nil },
		slog.New(slog.NewTextHandler(io.Discard, nil)),
		WithAPIBase(f.srv.URL),
		WithHTTPClient(&http.Client{Timeout: 3 * time.Second}),
	)

	// Telegram puts the token in the URL path, so any error that quotes the
	// URL leaks the whole credential. Unlike WeCom's, it never appears in a
	// body that could be scrubbed separately — the only defence is not
	// including the URL in errors.
	err := c.Run(context.Background(), testBinding(), &collectSink{})
	if err == nil {
		t.Fatal("expected an error")
	}
	if strings.Contains(err.Error(), token) {
		t.Fatalf("the bot token leaked into an error: %v", err)
	}
	if strings.Contains(err.Error(), "super-secret") {
		t.Fatalf("part of the token leaked into an error: %v", err)
	}
}

func TestRunRequiresBindingAndSink(t *testing.T) {
	f := newFakeAPI(t)
	c := testChannel(t, f)

	if err := c.Run(context.Background(), nil, &collectSink{}); err == nil {
		t.Error("Run without a binding should fail")
	}
	if err := c.Run(context.Background(), testBinding(), nil); err == nil {
		t.Error("Run without a sink should fail")
	}
}

func TestRunReturnsOnCancellation(t *testing.T) {
	f := newFakeAPI(t)
	c := testChannel(t, f)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, testBinding(), &collectSink{}) }()

	// Let it authenticate and start polling before cancelling.
	waitFor(func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return len(f.calls) >= 2
	}, 3*time.Second)

	cancel()
	select {
	case err := <-done:
		if !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("Run error = %v, want context cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

// errAccept stands in for a pipeline refusal.
var errAccept = &APIError{Method: "accept", Code: 500, Description: "idempotency write failed"}
