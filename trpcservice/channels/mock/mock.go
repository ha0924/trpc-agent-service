// 设计依据：docs/IM通道接入设计.md §2「Channel 抽象」、§7.3「一期与后续」

// Package mock is a Channel implementation for local development and
// end-to-end testing.
//
// It is deliberately shaped like a real IM channel rather than like the
// shortest thing that works: it verifies a shared token, decodes into the same
// message type, returns a slice from Decode, ACKs separately from replying,
// and reports capabilities. Keeping the shape means adding WeCom later is a
// new package, not a change to Gateway or Worker.
package mock

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Name is the value stored in channel_bindings.channel.
const Name = "mock"

// SecretResolver turns a secret:// reference into its value. The channel
// receives credentials this way rather than holding them, so nothing here can
// end up in a struct dump or a log line.
type SecretResolver func(ref string) (string, error)

// Channel implements both halves of the platform Channel contract.
type Channel struct {
	client  *http.Client
	secrets SecretResolver
	// replyURL is where outbound messages are POSTed. A real channel would
	// call the platform's push API; the mock posts to a configured collector
	// so an end-to-end test can observe the reply.
	replyURL string
}

var (
	_ types.InboundChannel  = (*Channel)(nil)
	_ types.OutboundChannel = (*Channel)(nil)
	_ types.Channel         = (*Channel)(nil)
)

// Option configures the channel.
type Option func(*Channel)

// WithReplyURL sets the outbound collector endpoint.
func WithReplyURL(u string) Option { return func(c *Channel) { c.replyURL = u } }

// WithHTTPClient overrides the client, for tests.
func WithHTTPClient(cl *http.Client) Option { return func(c *Channel) { c.client = cl } }

// New builds the mock channel.
func New(secrets SecretResolver, opts ...Option) *Channel {
	c := &Channel{
		client:  &http.Client{Timeout: 10 * time.Second},
		secrets: secrets,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// ID identifies the channel. Part of openclaw's Channel interface.
func (c *Channel) ID() string { return Name }

// Run blocks until ctx is done. Part of openclaw's Channel interface.
//
// The mock has no long-poll loop — it is driven entirely by inbound HTTP —
// so it just waits for shutdown. A Telegram channel would poll here.
func (c *Channel) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// Capabilities reports the channel's defaults. Per-binding values from
// channel_bindings.capabilities override these.
func (c *Channel) Capabilities() types.Capabilities {
	return types.Capabilities{
		InboundMode:     types.InboundModePayload,
		SupportsPush:    true,
		SupportsEdit:    false,
		MaxTextLength:   2048,
		RateLimitPerMin: 60,
	}
}

// inboundPayload is the mock's wire format.
type inboundPayload struct {
	// EventID is the platform's message identifier and the idempotency key.
	// When absent the channel generates one, which makes ad-hoc curl testing
	// convenient but means such requests are not deduplicated.
	EventID string `json:"event_id"`
	UserID  string `json:"user_id"`
	GroupID string `json:"group_id,omitempty"`
	Text    string `json:"text"`
}

// Verify authenticates the request against the binding's shared token.
//
// It runs before Decode because an unverified body may be attacker-supplied.
// A binding without a secret reference skips verification, which keeps local
// development frictionless; a real channel would reject that configuration.
func (c *Channel) Verify(r *http.Request, binding *types.ChannelBinding) error {
	if binding == nil {
		return fmt.Errorf("mock: nil binding")
	}
	if binding.SecretRef == "" {
		return nil
	}

	want, err := c.secrets(binding.SecretRef)
	if err != nil {
		// A binding that names a secret we cannot resolve is a configuration
		// error. Failing closed is the only safe response: skipping
		// verification would accept forged callbacks.
		return fmt.Errorf("mock: resolve secret for %s: %w", binding.ChannelBindingID, err)
	}
	if want == "" {
		return nil
	}

	got := r.Header.Get("X-Mock-Token")
	// Constant-time compare so a caller cannot learn the token byte by byte
	// from response timing.
	if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
		return fmt.Errorf("mock: token mismatch for binding %s", binding.ChannelBindingID)
	}
	return nil
}

// Decode turns a verified request into platform messages.
//
// It returns a slice because fetch-mode channels pull a batch per callback.
// The mock always yields exactly one, but keeping the signature uniform means
// Gateway needs no per-channel branching.
func (c *Channel) Decode(ctx context.Context, r *http.Request, binding *types.ChannelBinding) ([]types.InboundMessage, error) {
	if binding == nil {
		return nil, fmt.Errorf("mock: nil binding")
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("mock: read body: %w", err)
	}

	var p inboundPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return nil, fmt.Errorf("mock: parse body: %w", err)
	}
	if p.UserID == "" {
		return nil, fmt.Errorf("mock: user_id is required")
	}
	if p.Text == "" {
		return nil, fmt.Errorf("mock: text is required")
	}

	// Group membership decides which conversation this belongs to. A user
	// speaking in two different groups must land in two different sessions.
	scope, scopeKey := types.ScopeSingle, p.UserID
	if p.GroupID != "" {
		scope, scopeKey = types.ScopeGroup, p.GroupID
	}

	eventID := p.EventID
	if eventID == "" {
		eventID = "mock-" + uuid.NewString()
	}

	return []types.InboundMessage{{
		Channel:          Name,
		ChannelBindingID: binding.ChannelBindingID,
		TenantID:         binding.TenantID,
		AgentAppID:       binding.AgentAppID,
		ExternalUserID:   p.UserID,
		ExternalGroupID:  p.GroupID,
		Scope:            scope,
		ScopeKey:         scopeKey,
		ExternalEventID:  eventID,
		Text:             p.Text,
		ReceivedAt:       time.Now(),
	}}, nil
}

// ackBody is what the platform sees immediately.
type ackBody struct {
	OK        bool   `json:"ok"`
	RequestID string `json:"request_id,omitempty"`
	TraceID   string `json:"trace_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	Duplicate bool   `json:"duplicate,omitempty"`
}

// Ack writes the immediate response.
//
// It is separate from the reply because the agent has not run yet and will not
// finish within an IM platform's callback timeout. The actual answer arrives
// later through Send.
func (c *Channel) Ack(w http.ResponseWriter, r *http.Request, binding *types.ChannelBinding, info types.AckInfo) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	return json.NewEncoder(w).Encode(ackBody{
		OK:        true,
		RequestID: info.RequestID,
		TraceID:   info.TraceID,
		SessionID: info.SessionID,
		Duplicate: info.Duplicate,
	})
}

// outboundPayload is what the reply collector receives.
type outboundPayload struct {
	Target    string    `json:"target"`
	Text      string    `json:"text"`
	Files     []string  `json:"files,omitempty"`
	Timestamp time.Time `json:"timestamp"`
}

// Send delivers a reply.
//
// A real channel would acquire an access token and call the platform's push
// API here. The mock POSTs to a collector so an end-to-end test can assert the
// user actually received something.
func (c *Channel) Send(ctx context.Context, target string, msg types.OutboundMessage, binding *types.ChannelBinding) error {
	if c.replyURL == "" {
		// No collector configured: nothing to deliver to. Report success so
		// local runs without a collector still exercise the full path.
		return nil
	}

	payload := outboundPayload{Target: target, Text: msg.Text, Timestamp: time.Now()}
	for _, f := range msg.Files {
		payload.Files = append(payload.Files, f.Path)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("mock: marshal outbound: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.replyURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("mock: build outbound request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("mock: deliver to %s: %w", c.replyURL, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))

	if resp.StatusCode >= 300 {
		return fmt.Errorf("mock: collector returned %s", resp.Status)
	}
	return nil
}

// SendText satisfies openclaw's TextSender for callers holding the openclaw
// interface rather than the platform one.
func (c *Channel) SendText(ctx context.Context, target, text string) error {
	return c.Send(ctx, target, types.NewTextReply(text), nil)
}
