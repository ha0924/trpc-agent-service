// 设计依据：docs/IM通道接入设计.md §2「Channel 抽象」、§7「通道接入差异」

// Package wecom implements the WeCom (企业微信) channel.
//
// It is the counterpart to the mock channel and exists to prove the Channel
// abstraction holds against a real platform. The differences it has to absorb,
// none of which the mock exercises:
//
//   - The callback URL is verified by a GET carrying an encrypted echostr,
//     which must be decrypted and echoed back in plaintext.
//   - Every callback is signed and AES-encrypted, so the idempotency key lives
//     inside the ciphertext: deduplication cannot happen before decryption.
//   - Replies need an access token that expires and that a concurrent refresh
//     can invalidate.
//   - The passive response has a few seconds' budget, far less than an agent
//     run, so the reply must go out through the push API instead.
//
// All of that is contained here. Gateway and Worker treat WeCom exactly like
// the mock channel.
package wecom

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Name is the value stored in channel_bindings.channel.
const Name = "wecom"

// SecretResolver turns a secret:// reference into its value.
type SecretResolver func(ref string) (string, error)

// Channel implements both halves of the platform Channel contract for WeCom.
type Channel struct {
	secrets SecretResolver
	tokens  *TokenManager
	http    *http.Client
	log     *slog.Logger
}

var (
	_ types.InboundChannel  = (*Channel)(nil)
	_ types.OutboundChannel = (*Channel)(nil)
	_ types.Channel         = (*Channel)(nil)
)

// New builds the WeCom channel.
func New(secrets SecretResolver, tokens *TokenManager, logger *slog.Logger) *Channel {
	if logger == nil {
		logger = slog.Default()
	}
	return &Channel{
		secrets: secrets,
		tokens:  tokens,
		http:    &http.Client{Timeout: 10 * time.Second},
		log:     logger,
	}
}

// ID identifies the channel. Part of openclaw's Channel interface.
func (c *Channel) ID() string { return Name }

// Run blocks until ctx is done. WeCom is callback-driven, so there is no poll
// loop to run.
func (c *Channel) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Capabilities reports WeCom's traits.
//
// SupportsPush is what makes the ACK-then-reply design viable here: the
// passive response window is far shorter than an agent run.
func (c *Channel) Capabilities() types.Capabilities {
	return types.Capabilities{
		InboundMode:  types.InboundModePayload,
		SupportsPush: true,
		// WeCom can recall a message but not edit one in place.
		SupportsEdit: false,
		// The text message body is limited to roughly 2048 bytes; longer
		// replies are split at paragraph boundaries by the platform.
		MaxTextLength:   2048,
		RateLimitPerMin: 60,
	}
}

// credentials resolves and parses the binding's credential blob.
//
// Read per call rather than cached on the struct: one Channel instance serves
// every tenant's bindings, so holding one tenant's secret in a field would be
// both wrong and a leak waiting to happen.
func (c *Channel) credentials(binding *types.ChannelBinding) (*Credentials, error) {
	if binding == nil {
		return nil, fmt.Errorf("wecom: nil binding")
	}
	if binding.SecretRef == "" {
		return nil, fmt.Errorf("wecom: binding %s has no secret reference", binding.ChannelBindingID)
	}
	raw, err := c.secrets(binding.SecretRef)
	if err != nil {
		return nil, fmt.Errorf("wecom: resolve secret for %s: %w", binding.ChannelBindingID, err)
	}
	return ParseCredentials(raw)
}

func (c *Channel) crypto(binding *types.ChannelBinding) (*Crypto, *Credentials, error) {
	creds, err := c.credentials(binding)
	if err != nil {
		return nil, nil, err
	}
	crypt, err := NewCrypto(creds.Token, creds.EncodingAESKey, creds.CorpID)
	if err != nil {
		return nil, nil, err
	}
	return crypt, creds, nil
}

// Verify authenticates the callback signature.
//
// It runs before Decode and before anything is parsed: the body is
// attacker-controlled until this passes.
func (c *Channel) Verify(r *http.Request, binding *types.ChannelBinding) error {
	crypt, _, err := c.crypto(binding)
	if err != nil {
		return err
	}

	q := r.URL.Query()
	msgSignature := q.Get("msg_signature")
	timestamp := q.Get("timestamp")
	nonce := q.Get("nonce")
	if msgSignature == "" || timestamp == "" || nonce == "" {
		return fmt.Errorf("wecom: callback is missing signature parameters")
	}

	// The URL-verification handshake signs echostr; message callbacks sign
	// the Encrypt element of the body. Both go through the same check.
	if echo := q.Get("echostr"); echo != "" {
		return crypt.VerifySignature(msgSignature, timestamp, nonce, echo)
	}

	body, err := readAndRestore(r)
	if err != nil {
		return err
	}
	var env envelope
	if err := xml.Unmarshal(body, &env); err != nil {
		return fmt.Errorf("wecom: parse callback envelope: %w", err)
	}
	if env.Encrypt == "" {
		return fmt.Errorf("wecom: callback carried no encrypted payload")
	}
	return crypt.VerifySignature(msgSignature, timestamp, nonce, env.Encrypt)
}

// envelope is the outer, unencrypted wrapper of a WeCom callback.
type envelope struct {
	XMLName    xml.Name `xml:"xml"`
	ToUserName string   `xml:"ToUserName"`
	AgentID    string   `xml:"AgentID"`
	Encrypt    string   `xml:"Encrypt"`
}

// inboundXML is the decrypted message.
type inboundXML struct {
	XMLName      xml.Name `xml:"xml"`
	ToUserName   string   `xml:"ToUserName"`
	FromUserName string   `xml:"FromUserName"`
	CreateTime   int64    `xml:"CreateTime"`
	MsgType      string   `xml:"MsgType"`
	Content      string   `xml:"Content"`
	MsgID        string   `xml:"MsgId"`
	AgentID      string   `xml:"AgentID"`
	Event        string   `xml:"Event"`
	// PicURL and MediaID appear on image messages.
	PicURL  string `xml:"PicUrl"`
	MediaID string `xml:"MediaId"`
}

// Decode turns a verified callback into platform messages.
//
// It returns an empty slice for the URL-verification handshake and for event
// callbacks that carry no user message; Gateway ACKs those without queueing
// anything.
func (c *Channel) Decode(ctx context.Context, r *http.Request, binding *types.ChannelBinding) ([]types.InboundMessage, error) {
	crypt, _, err := c.crypto(binding)
	if err != nil {
		return nil, err
	}

	// The verification handshake is a GET; it is answered in Ack, and decodes
	// to no messages.
	if r.Method == http.MethodGet {
		return nil, nil
	}

	body, err := readAndRestore(r)
	if err != nil {
		return nil, err
	}
	var env envelope
	if err := xml.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("wecom: parse callback envelope: %w", err)
	}

	plain, err := crypt.Decrypt(env.Encrypt)
	if err != nil {
		return nil, err
	}

	var in inboundXML
	if err := xml.Unmarshal(plain, &in); err != nil {
		return nil, fmt.Errorf("wecom: parse decrypted message: %w", err)
	}

	// Only text is handled in this phase. Other types are acknowledged and
	// dropped rather than erroring, so the platform stops retrying a message
	// we will never process.
	if in.MsgType != "text" || in.Content == "" {
		c.log.Debug("wecom callback ignored",
			"msg_type", in.MsgType, "event", in.Event,
			"binding_id", binding.ChannelBindingID)
		return nil, nil
	}

	// MsgId is the platform's identifier and therefore the idempotency key.
	// It is inside the ciphertext, which is why deduplication cannot happen
	// before decryption.
	eventID := in.MsgID
	if eventID == "" {
		// Event callbacks carry no MsgId. Falling back to sender plus
		// timestamp keeps deduplication working at one-second granularity.
		eventID = fmt.Sprintf("%s-%d", in.FromUserName, in.CreateTime)
	}

	return []types.InboundMessage{{
		Channel:          Name,
		ChannelBindingID: binding.ChannelBindingID,
		TenantID:         binding.TenantID,
		AgentAppID:       binding.AgentAppID,
		ExternalUserID:   in.FromUserName,
		Scope:            types.ScopeSingle,
		ScopeKey:         in.FromUserName,
		ExternalEventID:  eventID,
		Text:             in.Content,
		ReceivedAt:       time.Unix(in.CreateTime, 0),
	}}, nil
}

// Ack answers the callback.
//
// For the verification handshake it echoes the decrypted echostr, which is
// what proves to WeCom that we hold the key. For a message callback it returns
// an empty body: the real answer arrives later through the push API, because
// an agent run does not fit in the passive-response window.
func (c *Channel) Ack(w http.ResponseWriter, r *http.Request, binding *types.ChannelBinding, info types.AckInfo) error {
	if echo := r.URL.Query().Get("echostr"); echo != "" {
		crypt, _, err := c.crypto(binding)
		if err != nil {
			http.Error(w, "verification failed", http.StatusUnauthorized)
			return err
		}
		plain, err := crypt.Decrypt(echo)
		if err != nil {
			http.Error(w, "verification failed", http.StatusUnauthorized)
			return fmt.Errorf("wecom: decrypt echostr: %w", err)
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, err = w.Write(plain)
		return err
	}

	// An empty 200 tells WeCom the callback was accepted and stops its
	// retries. Anything else here would be treated as a passive reply.
	w.WriteHeader(http.StatusOK)
	return nil
}

// pushRequest is the body of the message/send API.
type pushRequest struct {
	ToUser  string   `json:"touser"`
	MsgType string   `json:"msgtype"`
	AgentID int64    `json:"agentid"`
	Text    pushText `json:"text"`
}

type pushText struct {
	Content string `json:"content"`
}

type pushResponse struct {
	ErrCode int    `json:"errcode"`
	ErrMsg  string `json:"errmsg"`
}

// errCodeInvalidToken and errCodeExpiredToken are the codes WeCom returns for
// a token it no longer accepts. They are handled by refreshing once rather
// than by retrying with the same value.
const (
	errCodeInvalidToken = 40014
	errCodeExpiredToken = 42001
)

// Send delivers a reply through the push API.
func (c *Channel) Send(ctx context.Context, target string, msg types.OutboundMessage, binding *types.ChannelBinding) error {
	creds, err := c.credentials(binding)
	if err != nil {
		return err
	}
	if c.tokens == nil {
		return fmt.Errorf("wecom: no token manager configured")
	}

	body := pushRequest{
		ToUser:  target,
		MsgType: "text",
		AgentID: creds.AgentID,
		Text:    pushText{Content: msg.Text},
	}

	err = c.push(ctx, binding.ChannelBindingID, creds, body)
	if isTokenError(err) {
		// The cached token was rejected before its cached expiry, most likely
		// because something else refreshed it. Drop it and try once more;
		// retrying with the same token would fail identically.
		c.log.Warn("wecom token rejected, refreshing once",
			"binding_id", binding.ChannelBindingID)
		c.tokens.Invalidate(ctx, binding.ChannelBindingID)
		err = c.push(ctx, binding.ChannelBindingID, creds, body)
	}
	return err
}

func (c *Channel) push(ctx context.Context, bindingID string, creds *Credentials, body pushRequest) error {
	token, err := c.tokens.Token(ctx, bindingID, creds)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("wecom: marshal push body: %w", err)
	}

	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/message/send?" +
		url.Values{"access_token": {token}}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("wecom: build push request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("wecom: push: %w", err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return fmt.Errorf("wecom: read push response: %w", err)
	}

	var out pushResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Errorf("wecom: parse push response: %w", err)
	}
	if out.ErrCode != 0 {
		return &apiError{Code: out.ErrCode, Msg: out.ErrMsg}
	}
	return nil
}

// SendText satisfies openclaw's TextSender.
func (c *Channel) SendText(ctx context.Context, target, text string) error {
	return fmt.Errorf("wecom: SendText requires a channel binding; use Send")
}

// apiError carries WeCom's error code so callers can branch on it rather than
// on message text, which is localised and changes.
type apiError struct {
	Code int
	Msg  string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("wecom api error: errcode=%d errmsg=%s", e.Code, e.Msg)
}

func isTokenError(err error) bool {
	var ae *apiError
	if !asAPIError(err, &ae) {
		return false
	}
	return ae.Code == errCodeInvalidToken || ae.Code == errCodeExpiredToken
}

func asAPIError(err error, target **apiError) bool {
	if err == nil {
		return false
	}
	if ae, ok := err.(*apiError); ok {
		*target = ae
		return true
	}
	return false
}

// readAndRestore reads the body and puts it back, so Verify and Decode can
// each read it. Without this the second reader would see an empty body.
func readAndRestore(r *http.Request) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("wecom: read body: %w", err)
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	return body, nil
}

// FormatSignatureParams renders the query parameters WeCom expects, for tests
// and for constructing replay fixtures.
func FormatSignatureParams(crypt *Crypto, encrypted string) url.Values {
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	nonce := "nonce" + ts
	return url.Values{
		"msg_signature": {crypt.Signature(ts, nonce, encrypted)},
		"timestamp":     {ts},
		"nonce":         {nonce},
	}
}
