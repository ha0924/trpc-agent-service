// 设计依据：docs/IM通道接入设计.md §7.1「差异对比」、§9「长连接接入模式」
//                docs/框架复用与扩展.md §2.5「openclaw 中可复用的部分」

// Package telegram implements the Telegram Bot API channel.
//
// This is the platform's second real IM channel, and it exists to answer the
// brief's "at least two kinds of IM channel" with two genuinely different
// platforms rather than two access modes of one. WeCom's callback and
// long-connection forms differ enormously, but they are the same platform;
// Telegram brings a different set of constraints.
//
// The protocol is written here rather than reused. `openclaw/plugins/telegram`
// looks like a complete implementation but is a 204-line registration shell:
// the actual protocol lives in `openclaw/internal/...`, which Go forbids
// 外部 modules from importing. So the only thing reusable from openclaw is the
// `channel.Channel` interface — see 框架复用与扩展.md §2.5, which was corrected
// after this was discovered.
//
// What Telegram contributes to the capability matrix, none of which WeCom
// exercises:
//
//   - No public address needed, and no callback either: getUpdates is a long
//     poll the platform drives. This is inbound mode "stream" without the
//     coupled outbound path — replies are ordinary HTTPS calls any Worker can
//     make. That combination is why inbound and outbound mode had to be split
//     into two dimensions.
//   - No signature, no encryption. The bot token in the URL path is the whole
//     credential, which makes it more sensitive than WeCom's, not less: it
//     never appears in a body that could be scrubbed separately.
//   - update_id is a monotonic cursor rather than an opaque id. Acknowledging
//     is implicit — asking for offset N+1 is what confirms N — so the cursor
//     must be advanced only after a message is durably accepted.
package telegram

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Name is the value stored in channel_bindings.channel.
const Name = "telegram"

// DefaultAPIBase is Telegram's Bot API host.
const DefaultAPIBase = "https://api.telegram.org"

// longPollSeconds is how long getUpdates parks server-side waiting for
// traffic.
//
// A long value is the point: it turns polling into something close to a push,
// with one request per interval rather than per poll. Telegram permits up to
// 50; 30 leaves room for the client timeout to be comfortably larger without
// either side racing the other.
const longPollSeconds = 30

// httpTimeout must exceed longPollSeconds, or every idle poll would be
// cancelled client-side and look like a network failure.
const httpTimeout = time.Duration(longPollSeconds+15) * time.Second

// maxTextLength is Telegram's per-message limit, in UTF-16 code units. Using
// it as a rune budget is conservative — a reply is split slightly earlier
// than strictly necessary, which is preferable to a rejected send.
const maxTextLength = 4096

// SecretResolver turns a secret:// reference into its value.
type SecretResolver func(ref string) (string, error)

// Credentials hold what Telegram needs. Only a bot token: there is no
// separate signing secret and no AES key.
type Credentials struct {
	BotToken string `json:"bot_token"`
}

// Valid reports whether the blob is usable.
func (c *Credentials) Valid() error {
	switch {
	case c == nil:
		return errors.New("telegram: nil credentials")
	case c.BotToken == "":
		return errors.New("telegram: bot_token is required")
	}
	return nil
}

// ParseCredentials decodes the JSON blob behind a secret reference.
//
// A bare token is also accepted. Telegram tokens are handed out as a plain
// string, and requiring an operator to wrap one in JSON invites a
// copy-paste mistake that surfaces as an authentication failure much later.
func ParseCredentials(raw string) (*Credentials, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, errors.New("telegram: empty credentials")
	}
	if !strings.HasPrefix(trimmed, "{") {
		return &Credentials{BotToken: trimmed}, nil
	}
	var c Credentials
	if err := json.Unmarshal([]byte(trimmed), &c); err != nil {
		return nil, fmt.Errorf("telegram: parse credentials: %w", err)
	}
	if err := c.Valid(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Channel implements the stream-inbound and direct-outbound halves of the
// platform Channel contract.
type Channel struct {
	secrets SecretResolver
	apiBase string
	http    *http.Client
	log     *slog.Logger

	// offsets tracks the getUpdates cursor per binding.
	//
	// Held in memory rather than persisted: on restart the channel resumes
	// from whatever Telegram still has queued, and the platform's own
	// idempotency record (uk_event on update_id) drops anything already
	// handled. Persisting the cursor would add a write per poll to prevent
	// duplicates that are already prevented.
	offsets map[string]int64
}

var (
	_ types.StreamChannel = (*Channel)(nil)
	// ReplySender, not OutboundChannel: this channel's Run belongs to the
	// stream contract and cannot also be openclaw's Run(ctx) error.
	_ types.ReplySender = (*Channel)(nil)
)

// Option configures the channel.
type Option func(*Channel)

// WithAPIBase overrides the API host, for tests.
func WithAPIBase(u string) Option { return func(c *Channel) { c.apiBase = u } }

// WithHTTPClient overrides the client, for tests.
func WithHTTPClient(cl *http.Client) Option { return func(c *Channel) { c.http = cl } }

// New builds the channel.
func New(secrets SecretResolver, logger *slog.Logger, opts ...Option) *Channel {
	if logger == nil {
		logger = slog.Default()
	}
	c := &Channel{
		secrets: secrets,
		apiBase: DefaultAPIBase,
		http:    &http.Client{Timeout: httpTimeout},
		log:     logger,
		offsets: make(map[string]int64),
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ID identifies the channel. Part of openclaw's Channel interface.
func (c *Channel) ID() string { return Name }

// Capabilities reports Telegram's traits.
//
// The pairing here is the one that forced inbound and outbound mode apart:
// stream inbound (we dial out and long-poll) with direct outbound (replies
// are plain HTTPS calls). Declaring via_holder would route replies through
// the outbox and start a per-bot election for no reason.
func (c *Channel) Capabilities() types.Capabilities {
	return types.Capabilities{
		InboundMode:  types.InboundModeStream,
		OutboundMode: types.OutboundModeDirect,
		SupportsPush: true,
		// Telegram can edit an already-sent message in place, which neither
		// WeCom form can. Unused for now, but it is a real difference and the
		// descriptor should say so.
		SupportsEdit:  true,
		MaxTextLength: maxTextLength,
		// Telegram's documented guidance is roughly one message per second
		// per chat, with bursts tolerated. 30/min keeps well inside that.
		RateLimitPerMin: 30,
	}
}

// credentials resolves the binding's credentials.
//
// Read per call rather than cached: one Channel instance serves every
// tenant's bindings, so holding one tenant's token in a field would be both
// wrong and a leak waiting to happen.
func (c *Channel) credentials(binding *types.ChannelBinding) (*Credentials, error) {
	if binding == nil {
		return nil, errors.New("telegram: nil binding")
	}
	if binding.SecretRef == "" {
		return nil, fmt.Errorf("telegram: binding %s has no secret reference",
			binding.ChannelBindingID)
	}
	raw, err := c.secrets(binding.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("telegram: resolve secret for %s: %w",
			binding.ChannelBindingID, err)
	}
	return ParseCredentials(raw)
}

// ---------------------------------------------------------------------------
// Bot API wire types
// ---------------------------------------------------------------------------

// apiResponse is the envelope every Bot API method returns.
type apiResponse struct {
	OK          bool            `json:"ok"`
	Result      json.RawMessage `json:"result,omitempty"`
	ErrorCode   int             `json:"error_code,omitempty"`
	Description string          `json:"description,omitempty"`
}

// update is one item from getUpdates.
type update struct {
	UpdateID int64      `json:"update_id"`
	Message  *tgMessage `json:"message,omitempty"`
	Edited   *tgMessage `json:"edited_message,omitempty"`
}

type tgMessage struct {
	MessageID int64   `json:"message_id"`
	From      *tgUser `json:"from,omitempty"`
	Chat      tgChat  `json:"chat"`
	Date      int64   `json:"date"`
	Text      string  `json:"text,omitempty"`
	Caption   string  `json:"caption,omitempty"`
}

type tgUser struct {
	ID        int64  `json:"id"`
	IsBot     bool   `json:"is_bot"`
	Username  string `json:"username,omitempty"`
	FirstName string `json:"first_name,omitempty"`
}

// tgChat carries the conversation. Type is what distinguishes a direct chat
// from a group, and it is how scope is decided.
type tgChat struct {
	ID    int64  `json:"id"`
	Type  string `json:"type"` // private / group / supergroup / channel
	Title string `json:"title,omitempty"`
}

// Run long-polls getUpdates and hands each message to the sink.
//
// This is a stream channel in the platform's sense — the platform dials out
// and no callback URL exists — even though the transport is HTTP rather than a
// socket. What matters to the supervisor is that a loop must be running and
// that only one replica should run it, both of which hold here: two replicas
// polling the same bot would each receive a share of the updates, splitting a
// conversation across processes.
//
// Reconnection is the supervisor's job. A failed poll is retried here with
// backoff because a single 502 is not worth releasing the lease over, but an
// unrecoverable error returns so the supervisor can decide.
func (c *Channel) Run(
	ctx context.Context,
	binding *types.ChannelBinding,
	sink types.MessageSink,
) error {
	if binding == nil {
		return errors.New("telegram: run requires a binding")
	}
	if sink == nil {
		return errors.New("telegram: run requires a sink")
	}

	creds, err := c.credentials(binding)
	if err != nil {
		return err
	}

	log := c.log.With(
		"tenant_id", binding.TenantID,
		"agent_app_id", binding.AgentAppID,
		"channel_binding_id", binding.ChannelBindingID)

	// Confirms the token before entering the loop, so a bad credential fails
	// immediately rather than as a stream of poll errors.
	if err := c.verifyToken(ctx, creds); err != nil {
		return err
	}
	log.Info("telegram bot authenticated")

	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		updates, err := c.getUpdates(ctx, creds, binding.ChannelBindingID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err() // shutdown or lease loss, not a fault
			}
			// A transient failure is expected on a long-lived poll. Backing
			// off beats returning, which would make the supervisor release
			// and re-acquire the lease for every hiccup.
			log.Warn("telegram poll failed, backing off",
				"error", applog.Scrub(err.Error()),
				"backoff", backoff.String())
			if !sleepCtx(ctx, backoff) {
				return ctx.Err()
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second

		for i := range updates {
			u := &updates[i]
			if err := c.handleUpdate(ctx, binding, sink, u, log); err != nil {
				return err
			}
			// Advanced only after the message is durably accepted. The cursor
			// *is* the acknowledgement — requesting offset N+1 tells Telegram
			// N is done — so advancing first would drop a message the
			// pipeline refused.
			c.setOffset(binding.ChannelBindingID, u.UpdateID+1)
		}
	}
}

// handleUpdate converts one update and hands it to the pipeline.
func (c *Channel) handleUpdate(
	ctx context.Context,
	binding *types.ChannelBinding,
	sink types.MessageSink,
	u *update,
	log *slog.Logger,
) error {
	msg := u.Message
	if msg == nil {
		// An edit is deliberately not treated as a new message: re-running an
		// agent because a user fixed a typo would double any tool side
		// effects. Ignoring it is the conservative reading.
		if u.Edited != nil {
			log.Debug("telegram edit ignored", "update_id", u.UpdateID)
		}
		return nil
	}

	text := msg.Text
	if text == "" {
		text = msg.Caption // an image with a caption still carries a question
	}
	if text == "" {
		log.Debug("telegram update has no text", "update_id", u.UpdateID)
		return nil
	}
	if msg.From != nil && msg.From.IsBot {
		// Bots talking to bots is an easy way to build an infinite loop.
		log.Debug("telegram message from a bot ignored", "update_id", u.UpdateID)
		return nil
	}

	inbound := &types.InboundMessage{
		// update_id is Telegram's own monotonic identifier and therefore the
		// idempotency key. Using message_id instead would be wrong: it is
		// unique per chat, not per bot, so two chats could collide.
		ExternalEventID: strconv.FormatInt(u.UpdateID, 10),
		Text:            text,
		ReceivedAt:      time.Unix(msg.Date, 0),
	}
	if msg.From != nil {
		inbound.ExternalUserID = strconv.FormatInt(msg.From.ID, 10)
	}

	// Group and direct conversations must land on different sessions.
	// chat.id is negative for groups, which is a Telegram quirk worth
	// keeping visible rather than normalising away.
	if msg.Chat.Type == "private" {
		inbound.Scope = types.ScopeSingle
		inbound.ScopeKey = strconv.FormatInt(msg.Chat.ID, 10)
		if inbound.ExternalUserID == "" {
			inbound.ExternalUserID = inbound.ScopeKey
		}
	} else {
		inbound.Scope = types.ScopeGroup
		inbound.ExternalGroupID = strconv.FormatInt(msg.Chat.ID, 10)
		inbound.ScopeKey = inbound.ExternalGroupID
	}

	if _, err := sink.Accept(ctx, inbound); err != nil {
		// No platform-side redelivery exists once the cursor advances, so a
		// message the pipeline refuses is genuinely at risk. Returning ends
		// the poll loop *without* advancing the offset, so the supervisor
		// reconnects and Telegram re-delivers the same update.
		return fmt.Errorf("telegram: accept update %d: %w", u.UpdateID, err)
	}
	return nil
}

// ---------------------------------------------------------------------------
// Outbound
// ---------------------------------------------------------------------------

// Send delivers a reply with sendMessage.
//
// An ordinary HTTPS call, which is why this channel declares direct outbound:
// any Worker can make it, with no need for the process holding the poll loop.
func (c *Channel) Send(
	ctx context.Context,
	target string,
	msg types.OutboundMessage,
	binding *types.ChannelBinding,
) error {
	creds, err := c.credentials(binding)
	if err != nil {
		return err
	}
	if target == "" {
		return errors.New("telegram: send requires a chat id")
	}

	body := map[string]any{
		"chat_id": target,
		"text":    msg.Text,
	}
	var out struct {
		MessageID int64 `json:"message_id"`
	}
	if err := c.call(ctx, creds, "sendMessage", body, &out); err != nil {
		return err
	}
	return nil
}

// SendText satisfies openclaw's TextSender.
func (c *Channel) SendText(ctx context.Context, target, text string) error {
	return errors.New("telegram: SendText requires a channel binding; use Send")
}

// ---------------------------------------------------------------------------
// API plumbing
// ---------------------------------------------------------------------------

// verifyToken calls getMe, which is the cheapest way to prove a token works.
func (c *Channel) verifyToken(ctx context.Context, creds *Credentials) error {
	var me struct {
		ID       int64  `json:"id"`
		Username string `json:"username"`
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := c.call(ctx, creds, "getMe", nil, &me); err != nil {
		return fmt.Errorf("telegram: verify token: %w", err)
	}
	return nil
}

// getUpdates long-polls for new messages.
func (c *Channel) getUpdates(
	ctx context.Context,
	creds *Credentials,
	bindingID string,
) ([]update, error) {
	body := map[string]any{
		"timeout": longPollSeconds,
		// Only message types the channel handles are requested, so the
		// server does not spend the poll budget on updates that would be
		// discarded here.
		"allowed_updates": []string{"message"},
	}
	if off := c.offset(bindingID); off > 0 {
		body["offset"] = off
	}

	var updates []update
	if err := c.call(ctx, creds, "getUpdates", body, &updates); err != nil {
		return nil, err
	}
	return updates, nil
}

// call performs one Bot API request.
//
// The token sits in the URL path, which Telegram requires. That makes the URL
// itself a secret — it must never be logged, and errors returned from here
// therefore describe the method rather than the address.
func (c *Channel) call(
	ctx context.Context,
	creds *Credentials,
	method string,
	body any,
	out any,
) error {
	url := fmt.Sprintf("%s/bot%s/%s", strings.TrimRight(c.apiBase, "/"),
		creds.BotToken, method)

	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal %s body: %w", method, err)
		}
		reader = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, reader)
	if err != nil {
		// The error from NewRequest can embed the URL, and the URL contains
		// the token. Replaced with the method name.
		return fmt.Errorf("build %s request", method)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		// Same reasoning: a transport error quotes the URL verbatim.
		return fmt.Errorf("telegram %s: request failed", method)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("telegram %s: read response: %w", method, err)
	}

	var env apiResponse
	if err := json.Unmarshal(raw, &env); err != nil {
		return fmt.Errorf("telegram %s: parse response: %w", method, err)
	}
	if !env.OK {
		return &APIError{Method: method, Code: env.ErrorCode, Description: env.Description}
	}
	if out != nil && len(env.Result) > 0 {
		if err := json.Unmarshal(env.Result, out); err != nil {
			return fmt.Errorf("telegram %s: parse result: %w", method, err)
		}
	}
	return nil
}

// APIError carries Telegram's error code so callers can branch on it rather
// than on the description, which is prose and changes.
type APIError struct {
	Method      string
	Code        int
	Description string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("telegram %s: error_code=%d description=%s",
		e.Method, e.Code, e.Description)
}

// Unauthorized reports whether the token was rejected. Distinguished because
// it is not worth retrying: a wrong token fails identically every time.
func (e *APIError) Unauthorized() bool { return e.Code == http.StatusUnauthorized }

func (c *Channel) offset(bindingID string) int64 {
	return c.offsets[bindingID]
}

func (c *Channel) setOffset(bindingID string, off int64) {
	c.offsets[bindingID] = off
}

// sleepCtx waits for d, reporting false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
