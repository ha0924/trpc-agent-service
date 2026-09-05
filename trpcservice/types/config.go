// 设计依据：docs/数据模型设计.md §5「核心表结构」
//                docs/多租户与节点部署设计.md §2「租户资源模型」

package types

import "time"

// Tenant is one company, department or business team, and the isolation
// boundary for every other resource.
type Tenant struct {
	TenantID string         `json:"tenant_id"`
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	Settings map[string]any `json:"settings,omitempty"`
}

// Active reports whether the tenant may serve traffic.
func (t *Tenant) Active() bool { return t != nil && t.Status == StatusActive }

// Status values shared by several configuration entities.
const (
	StatusActive    = "active"
	StatusSuspended = "suspended"
)

// AgentApp is one agent application, such as customer service or operations.
// It holds identity only; the runnable configuration lives in versions.
type AgentApp struct {
	TenantID    string `json:"tenant_id"`
	AgentAppID  string `json:"agent_app_id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Status      string `json:"status"`
}

// AgentVersion is one immutable configuration snapshot.
//
// A published version is never edited. Changing a prompt, a model, a tool, an
// extension, an MCP server or a skill all create a new version. Three
// properties follow: changes are traceable, rollback is a weight change, and
// a cached Runtime can never be stale.
type AgentVersion struct {
	TenantID       string         `json:"tenant_id"`
	AgentAppID     string         `json:"agent_app_id"`
	Version        string         `json:"version"`
	Status         string         `json:"status"`
	SystemPrompt   string         `json:"system_prompt"`
	ModelName      string         `json:"model_name"`
	ModelAPIKeyRef string         `json:"model_api_key_ref,omitempty"`
	ModelParams    map[string]any `json:"model_params,omitempty"`
	PublishedAt    *time.Time     `json:"published_at,omitempty"`
}

// Version status values.
const (
	VersionStatusDraft     = "draft"
	VersionStatusPublished = "published"
	VersionStatusArchived  = "archived"
)

// Published reports whether this version may serve traffic.
func (v *AgentVersion) Published() bool {
	return v != nil && v.Status == VersionStatusPublished
}

// Deployment records which versions serve one environment and in what
// proportion.
//
// One row per environment, with the weights inside Routes, rather than one
// row per version. A weight change or a rollback is then a single-row update
// and therefore atomic: there is no window in which the weights sum to
// something other than 100.
type Deployment struct {
	TenantID   string         `json:"tenant_id"`
	AgentAppID string         `json:"agent_app_id"`
	Env        string         `json:"env"`
	Routes     []VersionRoute `json:"routes"`
	Strategy   map[string]any `json:"strategy,omitempty"`
}

// VersionRoute is one version's share of traffic.
type VersionRoute struct {
	Version string `json:"version"`
	Weight  int    `json:"weight"`
}

// TotalWeight sums the configured weights, for validation on write.
func (d *Deployment) TotalWeight() int {
	total := 0
	for _, r := range d.Routes {
		total += r.Weight
	}
	return total
}

// ChannelBinding ties an external IM account to a tenant and an agent. It is
// what makes an inbound callback attributable: the request path identifies
// the binding, and the binding — not the payload — names the tenant.
type ChannelBinding struct {
	ChannelBindingID string `json:"channel_binding_id"`
	TenantID         string `json:"tenant_id"`
	AgentAppID       string `json:"agent_app_id"`
	Env              string `json:"env"`
	Channel          string `json:"channel"`
	ExternalAppID    string `json:"external_app_id,omitempty"`
	WebhookPath      string `json:"webhook_path,omitempty"`

	// SecretRef points at the credential in the secret manager. The plaintext
	// token never appears in this struct, in the database, or in any log.
	SecretRef string `json:"secret_ref,omitempty"`

	Capabilities Capabilities `json:"capabilities"`
	Status       string       `json:"status"`
}

// Session is the durable metadata of one conversation.
type Session struct {
	SessionID  string `json:"session_id"`
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id"`

	// AgentVersion is chosen by deployment weight when the session is created
	// and then frozen, so publishing or rolling back does not change the
	// behaviour of a conversation already under way.
	AgentVersion string `json:"agent_version"`

	Channel          string `json:"channel"`
	ChannelBindingID string `json:"channel_binding_id"`
	Scope            Scope  `json:"scope"`
	ScopeKey         string `json:"scope_key"`
	InternalUserID   string `json:"internal_user_id,omitempty"`

	// LastSequence is the highest event sequence written. Sequence uniqueness
	// per session is enforced by the database, which turns a concurrency bug
	// into a failed write instead of a corrupted conversation.
	LastSequence int64      `json:"last_sequence"`
	Status       string     `json:"status"`
	LastActiveAt *time.Time `json:"last_active_at,omitempty"`
}

// SessionEvent is one message or tool call inside a conversation.
type SessionEvent struct {
	TenantID     string    `json:"tenant_id"`
	SessionID    string    `json:"session_id"`
	Sequence     int64     `json:"sequence"`
	EventType    EventType `json:"event_type"`
	Role         string    `json:"role,omitempty"`
	Content      any       `json:"content,omitempty"`
	RequestID    string    `json:"request_id"`
	TraceID      string    `json:"trace_id,omitempty"`
	AgentVersion string    `json:"agent_version,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// EventType classifies a session event.
type EventType string

const (
	EventTypeUserMessage  EventType = "user_message"
	EventTypeAgentMessage EventType = "agent_message"
	EventTypeToolCall     EventType = "tool_call"
	EventTypeToolResult   EventType = "tool_result"
	EventTypeSystem       EventType = "system"
)

// InboundEvent is the idempotency record for one external message.
//
// Written before the ACK and before queueing, it is also the only reliable
// record of in-flight work: sweeping rows stuck in StateProcessing is how
// hints lost by the queue are recovered.
type InboundEvent struct {
	TenantID         string          `json:"tenant_id"`
	ChannelBindingID string          `json:"channel_binding_id"`
	ExternalEventID  string          `json:"external_event_id"`
	RequestID        string          `json:"request_id"`
	TraceID          string          `json:"trace_id,omitempty"`
	SessionID        string          `json:"session_id,omitempty"`
	Payload          *InboundMessage `json:"payload,omitempty"`
	State            InboundState    `json:"state"`
	Attempts         int             `json:"attempts"`
	LastError        string          `json:"last_error,omitempty"`
}

// InboundState is the two-phase lifecycle of an inbound event.
//
// Execution and delivery are separate states on purpose. Once the agent has
// run, a delivery failure must retry only the delivery: rerunning would
// repeat every tool call, which is unacceptable for tools with side effects.
type InboundState string

const (
	StateProcessing     InboundState = "processing"
	StateSucceeded      InboundState = "succeeded"
	StateDeliveryFailed InboundState = "delivery_failed"
	StateFailed         InboundState = "failed"
)
