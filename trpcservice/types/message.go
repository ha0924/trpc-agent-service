// 设计依据：docs/IM通道接入设计.md §3「统一消息模型」、§4「与框架输入输出的转换」

package types

import (
	"fmt"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	openclawchannel "trpc.group/trpc-go/trpc-agent-go/openclaw/channel"
	"trpc.group/trpc-go/trpc-agent-go/openclaw/gwproto"
)

// Scope distinguishes a one-to-one conversation from a group one. Together
// with ScopeKey it is what locates a session, and it maps directly onto the
// sessions table's uk_scope unique key.
type Scope string

const (
	// ScopeSingle is a direct message; ScopeKey is the external user id.
	ScopeSingle Scope = "single"
	// ScopeGroup is a group or thread; ScopeKey is the external group id.
	ScopeGroup Scope = "group"
)

// InboundMessage is one external message after a Channel has verified,
// decrypted and normalised it. Every channel produces this same shape, so
// Gateway's idempotency, routing and session logic has no per-channel
// branches.
//
// Media parts reuse gwproto.ContentPart instead of a platform-specific type:
// the image, audio, file, location and link variants are already defined
// there, and staying on that type keeps the conversion to model.Message
// mechanical.
type InboundMessage struct {
	// Channel origin. TenantID and AgentAppID are resolved by Gateway from
	// the channel binding, not supplied by the caller — an inbound payload is
	// untrusted and must never be able to name its own tenant.
	Channel          string `json:"channel"`
	ChannelBindingID string `json:"channel_binding_id"`
	TenantID         string `json:"tenant_id"`
	AgentAppID       string `json:"agent_app_id"`

	// Identity as seen by the external platform.
	ExternalUserID  string `json:"external_user_id"`
	ExternalGroupID string `json:"external_group_id,omitempty"`

	// Conversation scope.
	Scope    Scope  `json:"scope"`
	ScopeKey string `json:"scope_key"`

	// ExternalEventID is the platform's own event or message identifier and
	// is the idempotency key, unique per channel binding. For encrypted
	// channels it lives inside the ciphertext, so it is only available after
	// decryption — deduplication therefore cannot happen before decoding.
	ExternalEventID string `json:"external_event_id"`

	// Content. Text carries the plain-text form; Parts carries the structured
	// form when the message is multimodal.
	Text  string                `json:"text,omitempty"`
	Parts []gwproto.ContentPart `json:"parts,omitempty"`

	// CorrelationID is the platform's own token for this message, when the
	// platform requires the reply to echo it. Long-connection channels do:
	// WeCom's aibot protocol keys a reply to the req_id of the callback it
	// answers.
	//
	// It rides on the message so it survives the trip through the queue to
	// the Worker and back out on the reply — the platform layer never reads
	// or rewrites it. Empty for channels that reply over a fresh HTTP request
	// and need no correlation.
	CorrelationID string `json:"correlation_id,omitempty"`

	// Tracing.
	RequestID  string    `json:"request_id"`
	TraceID    string    `json:"trace_id"`
	ReceivedAt time.Time `json:"received_at"`
}

// OutboundMessage is a reply on its way back to an IM platform. It aliases
// openclaw's outbound type so channels implementing openclaw's TextSender or
// MessageSender need no adaptation.
type OutboundMessage = openclawchannel.OutboundMessage

// OutboundFile is one file attached to a reply.
type OutboundFile = openclawchannel.OutboundFile

// ToModelMessage converts an inbound message into the user message handed to
// runner.Run.
//
// When no structured parts are present the plain text form is used, which is
// the common case and keeps the request small. Parts that have no model-side
// equivalent (location, link) degrade to a textual description rather than
// being dropped, so the model still sees that the user sent something.
func (m *InboundMessage) ToModelMessage() model.Message {
	if len(m.Parts) == 0 {
		return model.NewUserMessage(m.Text)
	}

	parts := make([]model.ContentPart, 0, len(m.Parts))
	for _, p := range m.Parts {
		if converted, ok := convertPart(p); ok {
			parts = append(parts, converted)
		}
	}
	if len(parts) == 0 {
		return model.NewUserMessage(m.Text)
	}
	return model.Message{Role: model.RoleUser, Content: m.Text, ContentParts: parts}
}

// convertPart maps one gwproto content part onto its model equivalent.
func convertPart(p gwproto.ContentPart) (model.ContentPart, bool) {
	switch p.Type {
	case gwproto.PartTypeText:
		if p.Text == nil {
			return model.ContentPart{}, false
		}
		return model.ContentPart{Type: model.ContentTypeText, Text: p.Text}, true

	case gwproto.PartTypeImage:
		if p.Image == nil {
			return model.ContentPart{}, false
		}
		return model.ContentPart{
			Type: model.ContentTypeImage,
			Image: &model.Image{
				URL:    p.Image.URL,
				Data:   p.Image.Data,
				Detail: p.Image.Detail,
				Format: p.Image.Format,
			},
		}, true

	case gwproto.PartTypeFile, gwproto.PartTypeVideo:
		if p.File == nil {
			return model.ContentPart{}, false
		}
		return model.ContentPart{
			Type: model.ContentTypeFile,
			File: &model.File{
				Name:     p.File.Filename,
				URL:      p.File.URL,
				Data:     p.File.Data,
				FileID:   p.File.FileID,
				MimeType: p.File.Format,
			},
		}, true

	case gwproto.PartTypeLocation:
		if p.Location == nil {
			return model.ContentPart{}, false
		}
		return textPart(fmt.Sprintf("[位置] %s (%.6f, %.6f)",
			p.Location.Name, p.Location.Latitude, p.Location.Longitude)), true

	case gwproto.PartTypeLink:
		if p.Link == nil {
			return model.ContentPart{}, false
		}
		return textPart(fmt.Sprintf("[链接] %s %s", p.Link.Title, p.Link.URL)), true

	default:
		// Audio and voice are accepted by the model model but are out of
		// scope for phase one; dropping them here is preferable to sending a
		// half-formed part.
		return model.ContentPart{}, false
	}
}

func textPart(s string) model.ContentPart {
	return model.ContentPart{Type: model.ContentTypeText, Text: &s}
}

// NewTextReply builds a plain-text outbound message.
func NewTextReply(text string) OutboundMessage {
	return OutboundMessage{Text: text}
}
