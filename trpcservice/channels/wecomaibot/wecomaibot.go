// 设计依据：docs/IM通道接入设计.md §9「长连接接入模式」、§9.2「协议要点（企业微信 aibot）」

// Package wecomaibot implements the WeCom smart-robot (智能机器人) channel in
// long-connection mode.
//
// It is the counterpart to package wecom, which speaks the URL-callback form
// of the same product. The two are deliberately separate packages rather than
// one with a mode switch, because almost nothing is shared: this one has no
// signature verification, no AES envelope, no access token and no HTTP
// handler. What it has instead:
//
//   - A WebSocket the platform dials outward, so no public address is needed.
//   - A subscribe frame that doubles as authentication (BotID + Secret).
//   - A 30-second ping the server expects, absent which it hangs up.
//   - Replies written back into the same socket, echoing the platform's
//     req_id, which is why replies cannot be sent from a Worker.
//   - A disconnect event announcing that another connection has displaced
//     this one.
//
// Message payloads are plaintext here, so the callback form's rule — decrypt
// before deduplicating — does not apply: body.msgid is readable directly.
package wecomaibot

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Name is the value stored in channel_bindings.channel.
const Name = "wecom_aibot"

// DefaultEndpoint is WeCom's long-connection address.
const DefaultEndpoint = "wss://openws.work.weixin.qq.com"

// Protocol command names. Inbound and outbound frames share one envelope
// shape and are told apart by cmd.
const (
	cmdSubscribe   = "aibot_subscribe"
	cmdPing        = "ping"
	cmdMsgCallback = "aibot_msg_callback"
	cmdEvtCallback = "aibot_event_callback"
	cmdRespondMsg  = "aibot_respond_msg"
	cmdSendMsg     = "aibot_send_msg"
)

// Event types carried inside an event callback.
const (
	eventEnterChat    = "enter_chat"
	eventDisconnected = "disconnected_event"
	eventTemplateCard = "template_card_event"
	eventFeedback     = "feedback_event"
)

// pingInterval is the heartbeat WeCom documents as required. Sending less
// often risks the server closing the connection as idle.
const pingInterval = 30 * time.Second

// writeTimeout bounds a single frame write. A write that blocks forever would
// pin the sender loop and stall every reply for this bot.
const writeTimeout = 10 * time.Second

// readTimeout must exceed pingInterval comfortably: the read loop is idle
// between pushes, and a deadline shorter than the heartbeat would tear down a
// healthy connection.
const readTimeout = 90 * time.Second

// SecretResolver turns a secret:// reference into its value.
type SecretResolver func(ref string) (string, error)

// Credentials are the long-connection credentials, which differ from the
// callback form's: BotID and Secret rather than Token and EncodingAESKey.
type Credentials struct {
	BotID  string `json:"bot_id"`
	Secret string `json:"secret"`
}

// Valid reports whether the blob is usable.
func (c *Credentials) Valid() error {
	switch {
	case c == nil:
		return errors.New("wecom_aibot: nil credentials")
	case c.BotID == "":
		return errors.New("wecom_aibot: bot_id is required")
	case c.Secret == "":
		return errors.New("wecom_aibot: secret is required")
	}
	return nil
}

// ParseCredentials decodes the JSON blob behind a secret reference.
func ParseCredentials(raw string) (*Credentials, error) {
	var c Credentials
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, fmt.Errorf("wecom_aibot: parse credentials: %w", err)
	}
	if err := c.Valid(); err != nil {
		return nil, err
	}
	return &c, nil
}

// frame is the request envelope: every command shares this shape.
type frame struct {
	Cmd     string          `json:"cmd"`
	Headers frameHeaders    `json:"headers"`
	Body    json.RawMessage `json:"body,omitempty"`
}

type frameHeaders struct {
	ReqID string `json:"req_id"`
}

// response is the reply envelope for commands the platform sends us.
type response struct {
	Headers frameHeaders    `json:"headers"`
	ErrCode int             `json:"errcode"`
	ErrMsg  string          `json:"errmsg"`
	Body    json.RawMessage `json:"body,omitempty"`
	// Cmd is present on server-initiated pushes and absent on command
	// acknowledgements, which is how the read loop tells them apart.
	Cmd string `json:"cmd,omitempty"`
}

// subscribeBody authenticates the connection.
type subscribeBody struct {
	BotID  string `json:"bot_id"`
	Secret string `json:"secret"`
}

// msgCallbackBody is an inbound user message.
type msgCallbackBody struct {
	MsgID    string          `json:"msgid"`
	AIBotID  string          `json:"aibotid"`
	ChatID   string          `json:"chatid"`
	ChatType string          `json:"chattype"`
	From     fromField       `json:"from"`
	MsgType  string          `json:"msgtype"`
	Text     *textContent    `json:"text,omitempty"`
	Voice    *textContent    `json:"voice,omitempty"`
	Mixed    *mixedContent   `json:"mixed,omitempty"`
	Image    *mediaContent   `json:"image,omitempty"`
	File     *mediaContent   `json:"file,omitempty"`
	Video    *mediaContent   `json:"video,omitempty"`
	Quote    json.RawMessage `json:"quote,omitempty"`
}

type fromField struct {
	UserID string `json:"userid"`
}

type textContent struct {
	Content string `json:"content"`
}

type mediaContent struct {
	URL string `json:"url"`
	// AESKey is per-URL here, unlike the callback form's single
	// EncodingAESKey. Media handling is out of scope for this phase; the
	// field is parsed so the difference is visible in code rather than only
	// in the design doc.
	AESKey string `json:"aeskey"`
}

type mixedContent struct {
	MsgItem []mixedItem `json:"msg_item"`
}

type mixedItem struct {
	MsgType string        `json:"msgtype"`
	Text    *textContent  `json:"text,omitempty"`
	Image   *mediaContent `json:"image,omitempty"`
}

// eventCallbackBody is an inbound event.
type eventCallbackBody struct {
	MsgID      string    `json:"msgid"`
	CreateTime int64     `json:"create_time"`
	AIBotID    string    `json:"aibotid"`
	ChatID     string    `json:"chatid"`
	ChatType   string    `json:"chattype"`
	From       fromField `json:"from"`
	Event      struct {
		EventType string `json:"eventtype"`
	} `json:"event"`
}

// respondBody is a reply to a message callback.
//
// Only the stream message type is used. The platform treats a single push
// with finish=true as a complete message, so a non-streaming platform maps
// onto it without a second code path — and this platform does not deliver
// streaming responses.
type respondBody struct {
	MsgType string        `json:"msgtype"`
	Stream  streamPayload `json:"stream"`
}

type streamPayload struct {
	ID      string `json:"id"`
	Finish  bool   `json:"finish"`
	Content string `json:"content"`
}

// sendBody is an unsolicited push, used when no correlation token is
// available — a redelivery after a reconnect, for instance, where the
// original req_id belongs to a connection that no longer exists.
type sendBody struct {
	ChatID   string `json:"chatid"`
	ChatType uint32 `json:"chat_type"`
	MsgType  string `json:"msgtype"`
	Markdown struct {
		Content string `json:"content"`
	} `json:"markdown"`
}

// Chat type values for sendBody. The platform also accepts 0 for
// auto-detection, but it resolves ambiguity by assuming a group, so being
// explicit avoids a direct message being treated as one.
const (
	chatTypeSingle uint32 = 1
	chatTypeGroup  uint32 = 2
)

// Channel implements the stream halves of the platform Channel contract.
type Channel struct {
	secrets  SecretResolver
	endpoint string
	dialer   *websocket.Dialer
	log      *slog.Logger

	// mu guards conns. One Channel instance serves every tenant's bindings,
	// so per-binding state must be keyed, never held in a bare field — the
	// same reason the callback channel resolves credentials per call.
	mu    sync.RWMutex
	conns map[string]*connection
}

// connection is one live socket plus the state a reply needs.
type connection struct {
	ws *websocket.Conn

	// writeMu serialises writes. Gorilla permits only one concurrent writer,
	// and here two are natural: the heartbeat and the reply sender.
	writeMu sync.Mutex

	// correlations maps our external event id to the platform's req_id.
	//
	// Needed because a reply must echo the req_id of the callback it answers,
	// while the platform layer keys everything by external event id. The map
	// is bounded by pruning on use and on reconnect; a req_id whose
	// connection is gone is useless anyway.
	corrMu       sync.Mutex
	correlations map[string]string
}

var (
	_ types.StreamChannel = (*Channel)(nil)
	_ types.StreamSender  = (*Channel)(nil)
)

// Option configures the channel.
type Option func(*Channel)

// WithEndpoint overrides the WebSocket address, for tests.
func WithEndpoint(u string) Option { return func(c *Channel) { c.endpoint = u } }

// WithDialer overrides the dialer, for tests.
func WithDialer(d *websocket.Dialer) Option { return func(c *Channel) { c.dialer = d } }

// New builds the channel.
func New(secrets SecretResolver, logger *slog.Logger, opts ...Option) *Channel {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Channel{
		secrets:  secrets,
		endpoint: DefaultEndpoint,
		dialer: &websocket.Dialer{
			HandshakeTimeout: 15 * time.Second,
			Proxy:            http.ProxyFromEnvironment,
		},
		log:   logger,
		conns: make(map[string]*connection),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ID identifies the channel. Part of openclaw's Channel interface.
func (c *Channel) ID() string { return Name }

// Capabilities reports the long-connection traits.
//
// The numbers are WeCom's documented limits for this product and differ from
// the callback channel's: a much larger text budget, and a per-conversation
// rate cap rather than a per-application one.
func (c *Channel) Capabilities() types.Capabilities {
	return types.Capabilities{
		InboundMode: types.InboundModeStream,
		// Replies must echo the callback's req_id, which only the connection
		// that received it can do — so they cannot leave from a Worker. This
		// is declared rather than inferred from the inbound mode: Telegram
		// also dials out yet replies over an ordinary HTTPS call.
		OutboundMode: types.OutboundModeViaHolder,
		SupportsPush: true,
		// Stream messages can be refreshed until finished, and template cards
		// can be updated, so edits are genuinely supported here.
		SupportsEdit:    true,
		MaxTextLength:   20480,
		RateLimitPerMin: 30,
	}
}

// credentials resolves the binding's credential blob.
//
// Read per call rather than cached: one instance serves every tenant, so
// holding one tenant's secret in a field would be both wrong and a leak
// waiting to happen.
func (c *Channel) credentials(binding *types.ChannelBinding) (*Credentials, error) {
	if binding == nil {
		return nil, errors.New("wecom_aibot: nil binding")
	}
	if binding.SecretRef == "" {
		return nil, fmt.Errorf("wecom_aibot: binding %s has no secret reference",
			binding.ChannelBindingID)
	}
	raw, err := c.secrets(binding.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("wecom_aibot: resolve secret for %s: %w",
			binding.ChannelBindingID, err)
	}
	return ParseCredentials(raw)
}

// Run dials, subscribes, and reads until ctx ends or the connection fails.
//
// Reconnection is the supervisor's job, not this method's: the supervisor
// holds the connection lease, and reconnecting here would mean reconnecting
// without re-checking whether we are still the elected holder. So a failure
// returns and lets the supervisor decide.
func (c *Channel) Run(
	ctx context.Context,
	binding *types.ChannelBinding,
	sink types.MessageSink,
) error {
	if binding == nil {
		return errors.New("wecom_aibot: run requires a binding")
	}
	if sink == nil {
		return errors.New("wecom_aibot: run requires a sink")
	}

	creds, err := c.credentials(binding)
	if err != nil {
		return err
	}

	log := c.log.With(
		"tenant_id", binding.TenantID,
		"agent_app_id", binding.AgentAppID,
		"channel_binding_id", binding.ChannelBindingID,
		"bot_id", creds.BotID)

	ws, _, err := c.dialer.DialContext(ctx, c.endpoint, nil)
	if err != nil {
		return fmt.Errorf("wecom_aibot: dial %s: %w", c.endpoint, err)
	}
	defer ws.Close()

	conn := &connection{ws: ws, correlations: make(map[string]string)}

	c.mu.Lock()
	c.conns[binding.ChannelBindingID] = conn
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.conns, binding.ChannelBindingID)
		c.mu.Unlock()
	}()

	if err := c.subscribe(ctx, conn, creds); err != nil {
		return err
	}
	log.Info("wecom aibot subscribed")

	// A connection-scoped context, so the heartbeat stops when the read loop
	// ends rather than only when the caller cancels. Without it a connection
	// that the server displaced would leave the heartbeat running and Run
	// would block on it forever — the read loop can end on its own, not just
	// on shutdown.
	runCtx, stop := context.WithCancel(ctx)
	defer stop()

	// Closing the socket is what unblocks a read parked on ReadMessage;
	// cancelling the context alone would not.
	go func() {
		<-runCtx.Done()
		_ = ws.Close()
	}()

	heartbeatDone := make(chan struct{})
	go func() {
		defer close(heartbeatDone)
		c.heartbeat(runCtx, conn, log)
	}()

	err = c.readLoop(runCtx, conn, binding, sink, log)
	stop()
	<-heartbeatDone
	return err
}

// subscribe sends the subscribe frame and waits for its acknowledgement.
//
// Subscription is authentication, so nothing may be processed before it
// succeeds. Repeated subscribes are documented as rate-limited, which is
// another reason reconnection is the supervisor's decision and not a loop
// here.
func (c *Channel) subscribe(ctx context.Context, conn *connection, creds *Credentials) error {
	body, err := json.Marshal(subscribeBody{BotID: creds.BotID, Secret: creds.Secret})
	if err != nil {
		return fmt.Errorf("wecom_aibot: marshal subscribe body: %w", err)
	}

	reqID := newReqID()
	if err := conn.write(frame{
		Cmd:     cmdSubscribe,
		Headers: frameHeaders{ReqID: reqID},
		Body:    body,
	}); err != nil {
		return err
	}

	// A bounded wait: without it a server that accepts the socket but never
	// answers would leave the binding neither connected nor failed, and the
	// supervisor would keep holding a lease for nothing.
	if err := conn.ws.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		return fmt.Errorf("wecom_aibot: set subscribe deadline: %w", err)
	}

	_, raw, err := conn.ws.ReadMessage()
	if err != nil {
		return fmt.Errorf("wecom_aibot: read subscribe response: %w", err)
	}

	var resp response
	if err := json.Unmarshal(raw, &resp); err != nil {
		return fmt.Errorf("wecom_aibot: parse subscribe response: %w", err)
	}
	if resp.ErrCode != 0 {
		// Credentials are wrong or the bot is misconfigured. Retrying would
		// fail identically, so this has to surface rather than be retried.
		return fmt.Errorf("wecom_aibot: subscribe rejected: errcode=%d errmsg=%s",
			resp.ErrCode, resp.ErrMsg)
	}
	return nil
}

// heartbeat sends the documented 30-second ping.
func (c *Channel) heartbeat(ctx context.Context, conn *connection, log *slog.Logger) {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := conn.write(frame{
				Cmd:     cmdPing,
				Headers: frameHeaders{ReqID: newReqID()},
			}); err != nil {
				// The read loop will see the same broken socket and return
				// the authoritative error; logging at debug avoids reporting
				// one failure twice.
				log.Debug("wecom aibot heartbeat failed",
					"error", applog.Scrub(err.Error()))
				return
			}
		}
	}
}

// readLoop dispatches server pushes until the connection ends.
func (c *Channel) readLoop(
	ctx context.Context,
	conn *connection,
	binding *types.ChannelBinding,
	sink types.MessageSink,
	log *slog.Logger,
) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := conn.ws.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return fmt.Errorf("wecom_aibot: set read deadline: %w", err)
		}

		_, raw, err := conn.ws.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // shutdown or lease loss, not a fault
			}
			return fmt.Errorf("wecom_aibot: read: %w", err)
		}

		var resp response
		if err := json.Unmarshal(raw, &resp); err != nil {
			// A frame we cannot parse is dropped rather than fatal: one bad
			// frame must not take down a bot's only connection.
			log.Warn("wecom aibot frame unparseable",
				"error", applog.Scrub(err.Error()))
			continue
		}

		switch resp.Cmd {
		case cmdMsgCallback:
			if err := c.handleMessage(ctx, conn, binding, sink, &resp, log); err != nil {
				return err
			}
		case cmdEvtCallback:
			if done := c.handleEvent(conn, binding, &resp, log); done {
				// Another connection displaced this one. Returning lets the
				// supervisor release the lease so the new holder is the only
				// one reading.
				return nil
			}
		case "":
			// An acknowledgement of something we sent (ping, respond). Errors
			// are worth noting; successes are noise.
			if resp.ErrCode != 0 {
				log.Warn("wecom aibot command failed",
					"errcode", resp.ErrCode, "errmsg", resp.ErrMsg)
			}
		default:
			log.Debug("wecom aibot frame ignored", "cmd", resp.Cmd)
		}
	}
}

// handleMessage converts one message callback and hands it to the pipeline.
func (c *Channel) handleMessage(
	ctx context.Context,
	conn *connection,
	binding *types.ChannelBinding,
	sink types.MessageSink,
	resp *response,
	log *slog.Logger,
) error {
	var body msgCallbackBody
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		log.Warn("wecom aibot message body unparseable",
			"error", applog.Scrub(err.Error()))
		return nil
	}

	text, ok := extractText(&body)
	if !ok {
		// Media-only messages are acknowledged by doing nothing. Erroring
		// would drop the connection over a message type this phase never
		// intended to answer.
		log.Debug("wecom aibot message has no text",
			"msg_type", body.MsgType, "msgid", body.MsgID)
		return nil
	}

	msg := &types.InboundMessage{
		ExternalUserID:  body.From.UserID,
		ExternalEventID: body.MsgID,
		Text:            text,
		// The platform's token for this exchange. It must reach the reply
		// unchanged, and it is remembered here because the reply is produced
		// by another process entirely.
		CorrelationID: resp.Headers.ReqID,
		ReceivedAt:    time.Now(),
	}

	if body.ChatType == "group" {
		msg.Scope = types.ScopeGroup
		msg.ExternalGroupID = body.ChatID
		msg.ScopeKey = body.ChatID
	} else {
		msg.Scope = types.ScopeSingle
		msg.ScopeKey = body.From.UserID
	}

	conn.rememberCorrelation(body.MsgID, resp.Headers.ReqID)

	if _, err := sink.Accept(ctx, msg); err != nil {
		// No platform-side redelivery exists for a long connection, so a
		// message the pipeline refuses is genuinely lost. Ending the
		// connection makes that visible and lets the supervisor reconnect.
		return fmt.Errorf("wecom_aibot: accept message %s: %w", body.MsgID, err)
	}
	return nil
}

// handleEvent processes an event callback, reporting whether the connection
// should end.
//
// Events are not queued. Two of them — entering a chat and clicking a
// template card — must be answered within five seconds, which is far less
// than an agent run; routing them through the queue would guarantee a
// timeout. Recognising them and declining to answer is honest, where
// pretending to handle them would look supported and never work.
func (c *Channel) handleEvent(
	conn *connection,
	binding *types.ChannelBinding,
	resp *response,
	log *slog.Logger,
) bool {
	var body eventCallbackBody
	if err := json.Unmarshal(resp.Body, &body); err != nil {
		log.Warn("wecom aibot event body unparseable",
			"error", applog.Scrub(err.Error()))
		return false
	}

	switch body.Event.EventType {
	case eventDisconnected:
		// Another connection subscribed and the server is about to hang up.
		log.Warn("wecom aibot connection displaced by a new one")
		return true

	case eventEnterChat:
		log.Debug("wecom aibot enter-chat event received; no welcome configured",
			"user_id", body.From.UserID)
		return false

	case eventTemplateCard, eventFeedback:
		log.Debug("wecom aibot interaction event not handled in this phase",
			"event_type", body.Event.EventType)
		return false

	default:
		log.Debug("wecom aibot event ignored", "event_type", body.Event.EventType)
		return false
	}
}

// SendReply writes a reply into the live connection for binding.
//
// Called only by the process holding the socket, after it takes the reply off
// the outbox. Failing when no connection is held is essential rather than
// defensive: it is what marks the delivery failed so the sweeper retries,
// instead of reporting a reply as sent into nothing.
func (c *Channel) SendReply(
	ctx context.Context,
	binding *types.ChannelBinding,
	reply *types.StreamReply,
) error {
	if binding == nil || reply == nil {
		return errors.New("wecom_aibot: send reply requires a binding and a reply")
	}

	c.mu.RLock()
	conn, ok := c.conns[binding.ChannelBindingID]
	c.mu.RUnlock()
	if !ok {
		return fmt.Errorf("wecom_aibot: no live connection for binding %s",
			binding.ChannelBindingID)
	}

	// Prefer answering the original callback: it needs no prior contact and
	// carries the platform's own correlation. Fall back to an unsolicited
	// push when the token is gone, which happens when the reply outlived the
	// connection that received the message.
	reqID := reply.CorrelationID
	if reqID == "" {
		reqID = conn.correlationFor(reply.ExternalEventID)
	}
	if reqID == "" {
		return c.sendUnsolicited(conn, reply)
	}
	return c.respond(conn, reqID, reply)
}

// respond answers a message callback with a finished stream message.
func (c *Channel) respond(conn *connection, reqID string, reply *types.StreamReply) error {
	// A single push with finish=true is a complete message. Content is
	// always the full text, never a delta — the protocol updates by
	// replacement, and this platform does not stream.
	body, err := json.Marshal(respondBody{
		MsgType: "stream",
		Stream: streamPayload{
			ID:      streamIDFor(reply),
			Finish:  true,
			Content: reply.Text,
		},
	})
	if err != nil {
		return fmt.Errorf("wecom_aibot: marshal respond body: %w", err)
	}

	if err := conn.write(frame{
		Cmd:     cmdRespondMsg,
		Headers: frameHeaders{ReqID: reqID},
		Body:    body,
	}); err != nil {
		return err
	}
	conn.forgetCorrelation(reply.ExternalEventID)
	return nil
}

// sendUnsolicited pushes a message outside any callback exchange.
func (c *Channel) sendUnsolicited(conn *connection, reply *types.StreamReply) error {
	b := sendBody{
		ChatID:   reply.Target,
		ChatType: chatTypeSingle,
		MsgType:  "markdown",
	}
	if reply.Scope == types.ScopeGroup {
		b.ChatType = chatTypeGroup
	}
	b.Markdown.Content = reply.Text

	body, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("wecom_aibot: marshal send body: %w", err)
	}
	return conn.write(frame{
		Cmd:     cmdSendMsg,
		Headers: frameHeaders{ReqID: newReqID()},
		Body:    body,
	})
}

// write serialises one frame onto the socket.
func (conn *connection) write(f frame) error {
	payload, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("wecom_aibot: marshal %s frame: %w", f.Cmd, err)
	}

	// Gorilla allows one concurrent writer, and the heartbeat and the reply
	// sender both write.
	conn.writeMu.Lock()
	defer conn.writeMu.Unlock()

	if err := conn.ws.SetWriteDeadline(time.Now().Add(writeTimeout)); err != nil {
		return fmt.Errorf("wecom_aibot: set write deadline: %w", err)
	}
	if err := conn.ws.WriteMessage(websocket.TextMessage, payload); err != nil {
		return fmt.Errorf("wecom_aibot: write %s frame: %w", f.Cmd, err)
	}
	return nil
}

func (conn *connection) rememberCorrelation(eventID, reqID string) {
	if eventID == "" || reqID == "" {
		return
	}
	conn.corrMu.Lock()
	defer conn.corrMu.Unlock()
	conn.correlations[eventID] = reqID
}

func (conn *connection) correlationFor(eventID string) string {
	conn.corrMu.Lock()
	defer conn.corrMu.Unlock()
	return conn.correlations[eventID]
}

func (conn *connection) forgetCorrelation(eventID string) {
	conn.corrMu.Lock()
	defer conn.corrMu.Unlock()
	delete(conn.correlations, eventID)
}

// extractText pulls the answerable text out of a message body.
//
// Voice arrives already transcribed, so it is treated as text. Mixed messages
// contribute their text items and drop their images, which is the same
// degradation the callback channel applies.
func extractText(body *msgCallbackBody) (string, bool) {
	switch body.MsgType {
	case "text":
		if body.Text != nil && body.Text.Content != "" {
			return body.Text.Content, true
		}
	case "voice":
		if body.Voice != nil && body.Voice.Content != "" {
			return body.Voice.Content, true
		}
	case "mixed":
		if body.Mixed == nil {
			return "", false
		}
		var out string
		for _, item := range body.Mixed.MsgItem {
			if item.MsgType == "text" && item.Text != nil {
				out += item.Text.Content
			}
		}
		if out != "" {
			return out, true
		}
	}
	return "", false
}

// streamIDFor derives the stream id from the message being answered.
//
// Derived rather than random so that a retry of the same reply updates the
// existing message instead of posting a second one — the protocol treats a
// repeated stream id as an update.
func streamIDFor(reply *types.StreamReply) string {
	if reply.ExternalEventID != "" {
		return "s-" + reply.ExternalEventID
	}
	if reply.RequestID != "" {
		return "s-" + reply.RequestID
	}
	return "s-" + newReqID()
}

func newReqID() string { return uuid.NewString() }
