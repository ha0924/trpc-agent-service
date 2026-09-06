package wecomaibot

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// The protocol is tested against a real WebSocket server rather than a fake
// transport. The parts most likely to be wrong — that subscribe is awaited
// before anything is processed, that a reply echoes the platform's req_id,
// that a displacement notice ends the connection — are all sequencing
// properties a fake would let pass.

// fakeServer is a stand-in for WeCom's endpoint.
type fakeServer struct {
	srv *httptest.Server

	mu sync.Mutex
	// received holds every frame the client sent, in order.
	received []frame
	// rejectSubscribe makes the subscribe handshake fail.
	rejectSubscribe bool
	// pushes are frames to send once subscribed.
	pushes []string

	subscribed chan struct{}
	conns      chan *websocket.Conn
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	f := &fakeServer{
		subscribed: make(chan struct{}, 1),
		conns:      make(chan *websocket.Conn, 1),
	}

	upgrader := websocket.Upgrader{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		f.conns <- ws
		defer ws.Close()

		for {
			_, raw, err := ws.ReadMessage()
			if err != nil {
				return
			}
			var fr frame
			if err := json.Unmarshal(raw, &fr); err != nil {
				continue
			}

			f.mu.Lock()
			f.received = append(f.received, fr)
			reject := f.rejectSubscribe
			pushes := f.pushes
			f.mu.Unlock()

			switch fr.Cmd {
			case cmdSubscribe:
				code, msg := 0, "ok"
				if reject {
					code, msg = 40001, "invalid secret"
				}
				_ = ws.WriteJSON(response{
					Headers: frameHeaders{ReqID: fr.Headers.ReqID},
					ErrCode: code, ErrMsg: msg,
				})
				if reject {
					return
				}
				select {
				case f.subscribed <- struct{}{}:
				default:
				}
				for _, p := range pushes {
					_ = ws.WriteMessage(websocket.TextMessage, []byte(p))
				}
			default:
				_ = ws.WriteJSON(response{
					Headers: frameHeaders{ReqID: fr.Headers.ReqID},
					ErrCode: 0, ErrMsg: "ok",
				})
			}
		}
	}))

	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) url() string {
	return "ws" + strings.TrimPrefix(f.srv.URL, "http")
}

func (f *fakeServer) frames() []frame {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]frame, len(f.received))
	copy(out, f.received)
	return out
}

func (f *fakeServer) setPushes(p ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pushes = p
}

// collectingSink records what the channel hands to the pipeline.
type collectingSink struct {
	mu  sync.Mutex
	got []*types.InboundMessage
	err error
}

func (s *collectingSink) Accept(ctx context.Context, msg *types.InboundMessage) (types.AckInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return types.AckInfo{}, s.err
	}
	s.got = append(s.got, msg)
	return types.AckInfo{RequestID: msg.RequestID}, nil
}

func (s *collectingSink) messages() []*types.InboundMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*types.InboundMessage, len(s.got))
	copy(out, s.got)
	return out
}

func testBinding() *types.ChannelBinding {
	return &types.ChannelBinding{
		ChannelBindingID: "bind-aibot-1",
		TenantID:         "tenant-demo",
		AgentAppID:       "app-1",
		Channel:          Name,
		SecretRef:        "secret://aibot",
		Status:           types.StatusActive,
	}
}

func testSecrets(t *testing.T) SecretResolver {
	t.Helper()
	blob, err := json.Marshal(Credentials{BotID: "aib-123", Secret: "s3cr3t"})
	if err != nil {
		t.Fatalf("marshal credentials: %v", err)
	}
	return func(ref string) (string, error) { return string(blob), nil }
}

func TestCapabilitiesReportStreamMode(t *testing.T) {
	c := New(testSecrets(t), nil)

	// This value is what makes the supervisor open a connection and the
	// Worker route replies through the outbox. Getting it wrong would make
	// the binding behave like a webhook one, with no connection and replies
	// sent from a process that holds no socket.
	if !c.Capabilities().StreamCapable() {
		t.Fatal("wecom_aibot must report stream mode")
	}
	if c.Capabilities().MaxTextLength != 20480 {
		t.Errorf("MaxTextLength = %d, want 20480", c.Capabilities().MaxTextLength)
	}
}

func TestSubscribeIsSentFirstAndAwaited(t *testing.T) {
	srv := newFakeServer(t)
	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, testBinding(), &collectingSink{}) }()

	select {
	case <-srv.subscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("subscribe frame never arrived")
	}

	frames := srv.frames()
	if len(frames) == 0 {
		t.Fatal("server received no frames")
	}
	// Subscription is authentication, so it must be the very first frame:
	// anything sent before it would be unauthenticated.
	if frames[0].Cmd != cmdSubscribe {
		t.Fatalf("first frame = %q, want %q", frames[0].Cmd, cmdSubscribe)
	}

	var body subscribeBody
	if err := json.Unmarshal(frames[0].Body, &body); err != nil {
		t.Fatalf("parse subscribe body: %v", err)
	}
	if body.BotID != "aib-123" || body.Secret != "s3cr3t" {
		t.Errorf("subscribe carried bot_id=%q secret set=%v, want the binding's credentials",
			body.BotID, body.Secret != "")
	}
	if frames[0].Headers.ReqID == "" {
		t.Error("subscribe frame must carry a req_id")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}
}

func TestSubscribeRejectionIsFatal(t *testing.T) {
	srv := newFakeServer(t)
	srv.mu.Lock()
	srv.rejectSubscribe = true
	srv.mu.Unlock()

	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))

	// Bad credentials fail identically on every retry, so this must surface
	// rather than spin. The supervisor decides what to do, and repeated
	// subscribes are rate-limited by the platform.
	err := c.Run(context.Background(), testBinding(), &collectingSink{})
	if err == nil {
		t.Fatal("Run should fail when subscribe is rejected")
	}
	if !strings.Contains(err.Error(), "subscribe rejected") {
		t.Errorf("error = %v, want it to mention subscribe rejection", err)
	}
}

func TestTextMessageReachesThePipeline(t *testing.T) {
	srv := newFakeServer(t)
	srv.setPushes(`{
		"cmd": "aibot_msg_callback",
		"headers": {"req_id": "platform-req-1"},
		"body": {
			"msgid": "msg-1",
			"aibotid": "aib-123",
			"chattype": "single",
			"from": {"userid": "user-7"},
			"msgtype": "text",
			"text": {"content": "hello robot"}
		}
	}`)

	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))
	sink := &collectingSink{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	deadline := time.Now().Add(3 * time.Second)
	for len(sink.messages()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	msgs := sink.messages()
	if len(msgs) != 1 {
		t.Fatalf("pipeline saw %d messages, want 1", len(msgs))
	}
	got := msgs[0]

	if got.Text != "hello robot" {
		t.Errorf("Text = %q, want %q", got.Text, "hello robot")
	}
	// msgid is the platform's deduplication key and is readable directly:
	// unlike the callback form, nothing is encrypted here.
	if got.ExternalEventID != "msg-1" {
		t.Errorf("ExternalEventID = %q, want %q", got.ExternalEventID, "msg-1")
	}
	// The correlation token has to be captured, or the reply cannot be
	// matched to this exchange.
	if got.CorrelationID != "platform-req-1" {
		t.Errorf("CorrelationID = %q, want %q", got.CorrelationID, "platform-req-1")
	}
	if got.Scope != types.ScopeSingle {
		t.Errorf("Scope = %q, want single", got.Scope)
	}
	if got.ScopeKey != "user-7" {
		t.Errorf("ScopeKey = %q, want %q", got.ScopeKey, "user-7")
	}
}

func TestGroupMessageUsesChatIDAsScopeKey(t *testing.T) {
	srv := newFakeServer(t)
	srv.setPushes(`{
		"cmd": "aibot_msg_callback",
		"headers": {"req_id": "platform-req-2"},
		"body": {
			"msgid": "msg-2",
			"chatid": "chat-99",
			"chattype": "group",
			"from": {"userid": "user-7"},
			"msgtype": "text",
			"text": {"content": "@bot hi"}
		}
	}`)

	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))
	sink := &collectingSink{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	deadline := time.Now().Add(3 * time.Second)
	for len(sink.messages()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	msgs := sink.messages()
	if len(msgs) != 1 {
		t.Fatalf("pipeline saw %d messages, want 1", len(msgs))
	}
	// Group and direct conversations must land on different sessions, which
	// is what scope plus scope_key encodes. Using the speaker's id for a
	// group would merge every member's turn into one private session.
	if msgs[0].Scope != types.ScopeGroup {
		t.Errorf("Scope = %q, want group", msgs[0].Scope)
	}
	if msgs[0].ScopeKey != "chat-99" {
		t.Errorf("ScopeKey = %q, want %q", msgs[0].ScopeKey, "chat-99")
	}
}

func TestDisconnectedEventEndsTheConnection(t *testing.T) {
	srv := newFakeServer(t)
	srv.setPushes(`{
		"cmd": "aibot_event_callback",
		"headers": {"req_id": "platform-req-3"},
		"body": {
			"msgid": "evt-1",
			"msgtype": "event",
			"event": {"eventtype": "disconnected_event"}
		}
	}`)

	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background(), testBinding(), &collectingSink{}) }()

	select {
	case err := <-done:
		// A new connection has displaced this one. Returning without error
		// lets the supervisor release the lease promptly; treating it as a
		// failure would log a routine failover as a fault, and continuing to
		// read would double-process every message.
		if err != nil {
			t.Fatalf("Run error on displacement = %v, want nil", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return after a disconnected event")
	}
}

func TestUnparseableFrameDoesNotKillTheConnection(t *testing.T) {
	srv := newFakeServer(t)
	srv.setPushes(`{not json at all`, `{
		"cmd": "aibot_msg_callback",
		"headers": {"req_id": "platform-req-4"},
		"body": {
			"msgid": "msg-4",
			"chattype": "single",
			"from": {"userid": "user-7"},
			"msgtype": "text",
			"text": {"content": "still here"}
		}
	}`)

	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))
	sink := &collectingSink{}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, testBinding(), sink) }()

	deadline := time.Now().Add(3 * time.Second)
	for len(sink.messages()) == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	// One malformed frame must not take down a bot's only connection: the
	// message after it still has to arrive.
	if len(sink.messages()) != 1 {
		t.Fatalf("pipeline saw %d messages after a bad frame, want 1", len(sink.messages()))
	}
}

func TestSendReplyEchoesTheCorrelationToken(t *testing.T) {
	srv := newFakeServer(t)
	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))
	binding := testBinding()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, binding, &collectingSink{}) }()

	select {
	case <-srv.subscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("never subscribed")
	}

	err := c.SendReply(ctx, binding, &types.StreamReply{
		ChannelBindingID: binding.ChannelBindingID,
		TenantID:         binding.TenantID,
		Target:           "user-7",
		Scope:            types.ScopeSingle,
		CorrelationID:    "platform-req-1",
		ExternalEventID:  "msg-1",
		Text:             "the answer",
	})
	if err != nil {
		t.Fatalf("SendReply: %v", err)
	}

	var respond *frame
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, fr := range srv.frames() {
			if fr.Cmd == cmdRespondMsg {
				f := fr
				respond = &f
				break
			}
		}
		if respond != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if respond == nil {
		t.Fatal("no respond frame reached the server")
	}

	// The platform matches a reply to its exchange by req_id. A generated
	// one would be rejected.
	if respond.Headers.ReqID != "platform-req-1" {
		t.Errorf("respond req_id = %q, want the inbound token %q",
			respond.Headers.ReqID, "platform-req-1")
	}

	var body respondBody
	if err := json.Unmarshal(respond.Body, &body); err != nil {
		t.Fatalf("parse respond body: %v", err)
	}
	if body.MsgType != "stream" {
		t.Errorf("msgtype = %q, want stream", body.MsgType)
	}
	// finish=true in one shot: this platform does not stream, and an
	// unfinished message would stay editable and never settle.
	if !body.Stream.Finish {
		t.Error("stream.finish must be true, the reply is complete")
	}
	if body.Stream.Content != "the answer" {
		t.Errorf("content = %q, want %q", body.Stream.Content, "the answer")
	}
	if body.Stream.ID == "" {
		t.Error("stream.id is required on first push")
	}
}

func TestSendReplyFallsBackToUnsolicitedPush(t *testing.T) {
	srv := newFakeServer(t)
	c := New(testSecrets(t), nil, WithEndpoint(srv.url()))
	binding := testBinding()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = c.Run(ctx, binding, &collectingSink{}) }()

	select {
	case <-srv.subscribed:
	case <-time.After(3 * time.Second):
		t.Fatal("never subscribed")
	}

	// No correlation token: the reply outlived the connection that received
	// the message, which happens after a reconnect. An unsolicited push is
	// the only way it can still be delivered.
	err := c.SendReply(ctx, binding, &types.StreamReply{
		ChannelBindingID: binding.ChannelBindingID,
		TenantID:         binding.TenantID,
		Target:           "chat-99",
		Scope:            types.ScopeGroup,
		Text:             "late answer",
	})
	if err != nil {
		t.Fatalf("SendReply: %v", err)
	}

	var send *frame
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		for _, fr := range srv.frames() {
			if fr.Cmd == cmdSendMsg {
				f := fr
				send = &f
				break
			}
		}
		if send != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if send == nil {
		t.Fatal("no send frame reached the server")
	}

	var body sendBody
	if err := json.Unmarshal(send.Body, &body); err != nil {
		t.Fatalf("parse send body: %v", err)
	}
	if body.ChatID != "chat-99" {
		t.Errorf("chatid = %q, want %q", body.ChatID, "chat-99")
	}
	// Being explicit matters: the platform resolves an unspecified chat type
	// by assuming a group, which would misroute a direct message.
	if body.ChatType != chatTypeGroup {
		t.Errorf("chat_type = %d, want %d for a group", body.ChatType, chatTypeGroup)
	}
}

func TestSendReplyFailsWithoutALiveConnection(t *testing.T) {
	c := New(testSecrets(t), nil, WithEndpoint("ws://127.0.0.1:0"))

	// The reversed reply path must not hide this: a reply handed to a replica
	// that holds no socket has to fail, so the delivery is marked failed and
	// retried rather than reported as sent.
	err := c.SendReply(context.Background(), testBinding(), &types.StreamReply{
		ChannelBindingID: "bind-aibot-1",
		Text:             "nowhere to go",
	})
	if err == nil {
		t.Fatal("SendReply should fail when no connection is held")
	}
	if !strings.Contains(err.Error(), "no live connection") {
		t.Errorf("error = %v, want it to mention the missing connection", err)
	}
}

func TestExtractText(t *testing.T) {
	cases := []struct {
		name string
		body msgCallbackBody
		want string
		ok   bool
	}{
		{
			name: "text",
			body: msgCallbackBody{MsgType: "text", Text: &textContent{Content: "hi"}},
			want: "hi", ok: true,
		},
		{
			// Voice arrives already transcribed, so it is answerable as text
			// rather than being dropped as unsupported media.
			name: "voice is transcribed",
			body: msgCallbackBody{MsgType: "voice", Voice: &textContent{Content: "spoken"}},
			want: "spoken", ok: true,
		},
		{
			name: "mixed keeps text and drops images",
			body: msgCallbackBody{MsgType: "mixed", Mixed: &mixedContent{MsgItem: []mixedItem{
				{MsgType: "text", Text: &textContent{Content: "look at "}},
				{MsgType: "image", Image: &mediaContent{URL: "u", AESKey: "k"}},
				{MsgType: "text", Text: &textContent{Content: "this"}},
			}}},
			want: "look at this", ok: true,
		},
		{
			// Media-only messages have no text to answer. Reporting false
			// lets the caller ignore them without dropping the connection.
			name: "image alone has no text",
			body: msgCallbackBody{MsgType: "image", Image: &mediaContent{URL: "u", AESKey: "k"}},
			want: "", ok: false,
		},
		{
			name: "empty text",
			body: msgCallbackBody{MsgType: "text", Text: &textContent{Content: ""}},
			want: "", ok: false,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := extractText(&c.body)
			if ok != c.ok {
				t.Fatalf("ok = %v, want %v", ok, c.ok)
			}
			if got != c.want {
				t.Errorf("text = %q, want %q", got, c.want)
			}
		})
	}
}

func TestStreamIDIsStableForRetries(t *testing.T) {
	reply := &types.StreamReply{ExternalEventID: "msg-1", RequestID: "req-1"}

	// A retry must update the existing message rather than post a second
	// one, and the protocol keys that on a repeated stream id.
	if a, b := streamIDFor(reply), streamIDFor(reply); a != b {
		t.Fatalf("stream id is not stable: %q then %q", a, b)
	}
	if streamIDFor(reply) == streamIDFor(&types.StreamReply{ExternalEventID: "msg-2"}) {
		t.Error("different messages must get different stream ids")
	}
}

func TestParseCredentialsRejectsIncompleteBlobs(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"missing secret", `{"bot_id": "aib-1"}`},
		{"missing bot id", `{"secret": "s"}`},
		{"not json", `nope`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Failing at parse beats connecting and being rejected: the
			// error names the misconfiguration instead of surfacing as an
			// opaque platform errcode.
			if _, err := ParseCredentials(c.raw); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}
