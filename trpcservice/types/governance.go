// 设计依据：docs/治理监控与安全设计.md
//                docs/技术设计方案.md §7.2「治理策略」、§7.3「Metrics、Trace 和审计」

package types

import (
	"context"
	"time"
)

// Decision is the outcome of a governance check.
type Decision string

const (
	// DecisionAllow means the action proceeds.
	DecisionAllow Decision = "allow"
	// DecisionDeny means the action is refused.
	DecisionDeny Decision = "deny"
	// DecisionAsk means the action needs human confirmation before it takes
	// effect. The audit record must be written before the side effect, not
	// after, or a crash mid-call leaves no trace of what was attempted.
	DecisionAsk Decision = "ask"
	// DecisionError means the check itself failed. It is distinct from deny:
	// a failed check is an operational problem, a denial is policy working.
	DecisionError Decision = "error"
)

// AuditEventType classifies what was audited.
type AuditEventType string

const (
	AuditAgentRun  AuditEventType = "agent_run"
	AuditToolCall  AuditEventType = "tool_call"
	AuditModelCall AuditEventType = "model_call"
	AuditGuardrail AuditEventType = "guardrail"
	AuditDelivery  AuditEventType = "delivery"
)

// AuditRecord answers who did what, through which channel, using which agent
// or tool, and why the platform allowed or refused it.
//
// The field set is fixed by the acceptance criteria: tenant, channel, user,
// session, agent name, tool name, decision, latency, error type, cost and
// trace id must all be expressible.
type AuditRecord struct {
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id,omitempty"`
	// AgentName is the concrete agent that ran, version included, so an audit
	// trail survives a rollback that changed which version serves traffic.
	AgentName string `json:"agent_name,omitempty"`

	Channel   string `json:"channel,omitempty"`
	UserID    string `json:"user_id,omitempty"`
	SessionID string `json:"session_id,omitempty"`
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id,omitempty"`

	EventType AuditEventType `json:"event_type"`
	ToolName  string         `json:"tool_name,omitempty"`
	Decision  Decision       `json:"decision"`
	Reason    string         `json:"reason,omitempty"`

	LatencyMS int64   `json:"latency_ms,omitempty"`
	ErrorType string  `json:"error_type,omitempty"`
	CostUSD   float64 `json:"cost_usd,omitempty"`

	// Detail holds already-redacted supplementary data. Anything placed here
	// must have passed through the redactor first: audit records are retained
	// far longer than logs and are read by more people.
	Detail map[string]any `json:"detail,omitempty"`
}

// NewAuditRecord seeds a record from the request identity, so no call site has
// to remember which identity fields an audit entry needs.
func NewAuditRecord(rc *RequestContext, eventType AuditEventType) *AuditRecord {
	r := &AuditRecord{EventType: eventType, Decision: DecisionAllow}
	if rc != nil {
		r.TenantID = rc.TenantID
		r.AgentAppID = rc.AgentAppID
		r.Channel = rc.Channel
		r.UserID = rc.UserID
		r.SessionID = rc.SessionID
		r.RequestID = rc.RequestID
		r.TraceID = rc.TraceID
	}
	return r
}

// UsageRecord is one model call's token and cost detail, kept for
// reconciliation.
//
// Budget enforcement does not read this table: summing details on every call
// is expensive, and concurrent Workers decrementing a budget need an atomic
// counter. The counter lives in Redis; this is the ledger it is checked
// against after the fact.
type UsageRecord struct {
	TenantID     string `json:"tenant_id"`
	AgentAppID   string `json:"agent_app_id"`
	AgentVersion string `json:"agent_version,omitempty"`
	SessionID    string `json:"session_id,omitempty"`
	RequestID    string `json:"request_id"`
	TraceID      string `json:"trace_id,omitempty"`

	ModelName        string  `json:"model_name"`
	PromptTokens     int     `json:"prompt_tokens"`
	CompletionTokens int     `json:"completion_tokens"`
	TotalTokens      int     `json:"total_tokens"`
	CostUSD          float64 `json:"cost_usd"`
	LatencyMS        int64   `json:"latency_ms,omitempty"`
}

// AuditSink persists audit records.
//
// An interface so the destination can change — SQL now, a log pipeline or
// ClickHouse later — without touching the policies that produce records.
type AuditSink interface {
	// Write records one decision.
	//
	// Implementations must be safe for concurrent use. Ordinary records may
	// be written asynchronously to keep them off the reply path, but records
	// for dangerous tools have to be durable before the side effect runs, so
	// a sink must not silently buffer everything.
	Write(ctx context.Context, r *AuditRecord) error
}

// ToolApproval is a pending confirmation for a dangerous tool call.
type ToolApproval struct {
	ApprovalID  string         `json:"approval_id"`
	TenantID    string         `json:"tenant_id"`
	AgentAppID  string         `json:"agent_app_id"`
	SessionID   string         `json:"session_id"`
	RequestID   string         `json:"request_id"`
	TraceID     string         `json:"trace_id,omitempty"`
	ToolName    string         `json:"tool_name"`
	ToolArgs    map[string]any `json:"tool_args,omitempty"`
	RequestedBy string         `json:"requested_by,omitempty"`
	DecidedBy   string         `json:"decided_by,omitempty"`
	State       ApprovalState  `json:"state"`
	Reason      string         `json:"reason,omitempty"`
	ExpiresAt   *time.Time     `json:"expires_at,omitempty"`
}

// ApprovalState is the lifecycle of a confirmation request.
type ApprovalState string

const (
	ApprovalPending  ApprovalState = "pending"
	ApprovalApproved ApprovalState = "approved"
	ApprovalRejected ApprovalState = "rejected"
	// ApprovalExpired means nobody answered in time. It is a distinct state
	// from rejected so an unanswered request is not mistaken for a refusal
	// when reviewing what happened.
	ApprovalExpired ApprovalState = "expired"
)

// ChannelUser maps an external IM identity to an internal one and carries the
// attributes permission checks read.
type ChannelUser struct {
	TenantID         string         `json:"tenant_id"`
	ChannelBindingID string         `json:"channel_binding_id"`
	ExternalUserID   string         `json:"external_user_id"`
	InternalUserID   string         `json:"internal_user_id"`
	DisplayName      string         `json:"display_name,omitempty"`
	Attributes       map[string]any `json:"attributes,omitempty"`
	Status           string         `json:"status"`
}

// Roles returns the user's roles, for permission checks.
func (u *ChannelUser) Roles() []string {
	if u == nil || u.Attributes == nil {
		return nil
	}
	raw, ok := u.Attributes["roles"]
	if !ok {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		out := make([]string, 0, len(v))
		for _, item := range v {
			if s, ok := item.(string); ok {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}

// HasRole reports whether the user holds a role.
func (u *ChannelUser) HasRole(role string) bool {
	for _, r := range u.Roles() {
		if r == role {
			return true
		}
	}
	return false
}

// TenantSettings are the per-tenant policy knobs stored in tenants.settings.
type TenantSettings struct {
	// DailyTokenBudget caps tokens per tenant per day. Zero means unlimited.
	DailyTokenBudget int64 `json:"daily_token_budget,omitempty"`
	// MonthlyTokenBudget caps tokens per calendar month. Zero means unlimited.
	MonthlyTokenBudget int64 `json:"monthly_token_budget,omitempty"`
	// MaxTokensPerRequest caps a single request. Zero means unlimited.
	MaxTokensPerRequest int `json:"max_tokens_per_request,omitempty"`
	// RateLimitPerMin caps inbound messages per minute. Zero means unlimited.
	RateLimitPerMin int `json:"rate_limit_per_min,omitempty"`
	// AllowedRoles restricts which IM user roles may talk to this tenant's
	// agents. Empty means everyone.
	AllowedRoles []string `json:"allowed_roles,omitempty"`
}
