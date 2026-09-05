// 设计依据：docs/技术设计方案.md §3.2「所有请求携带租户上下文」
//                docs/多租户与节点部署设计.md §3「租户隔离」

package types

import (
	"context"
	"errors"
)

// RequestContext is the tenant and tracing identity of a single inbound
// message. Gateway builds it once from a trusted channel binding, and it then
// travels unchanged through the queue, the Worker, the model, every Tool and
// every storage call.
//
// It is deliberately a value carried in context.Context rather than a
// parameter threaded by hand: a method signature that lacks context.Context
// cannot participate in isolation, tracing or cancellation, and retrofitting
// one later means touching every implementation. This is the one part of the
// design that genuinely cannot be added after the fact.
//
// The struct is JSON-serialisable because it crosses the process boundary
// between Gateway and Worker inside the queue payload.
type RequestContext struct {
	// Tenant scope. TenantID is the isolation boundary; every configuration
	// lookup and data access is scoped by it.
	TenantID   string `json:"tenant_id"`
	AgentAppID string `json:"agent_app_id"`

	// AgentVersion is frozen when the session is created and does not change
	// when a new version is published, so an in-flight conversation keeps a
	// consistent configuration.
	AgentVersion string `json:"agent_version"`

	// Channel origin.
	Channel          string `json:"channel"`
	ChannelBindingID string `json:"channel_binding_id"`

	// Identity and conversation.
	UserID    string `json:"user_id"`
	SessionID string `json:"session_id"`

	// RequestID identifies one execution; TraceID spans the whole chain from
	// the IM callback to the reply and is what makes Gateway and Worker logs
	// joinable.
	RequestID string `json:"request_id"`
	TraceID   string `json:"trace_id"`
}

// ErrNoRequestContext is returned when a context carries no RequestContext.
// Callers that require tenant scope must treat this as a hard failure rather
// than falling back to a default tenant.
var ErrNoRequestContext = errors.New("types: no request context")

// contextKey is unexported so no other package can overwrite the value.
type contextKey struct{}

// NewContext returns a copy of ctx carrying rc.
func NewContext(ctx context.Context, rc *RequestContext) context.Context {
	return context.WithValue(ctx, contextKey{}, rc)
}

// FromContext extracts the RequestContext carried by ctx.
func FromContext(ctx context.Context) (*RequestContext, error) {
	rc, ok := ctx.Value(contextKey{}).(*RequestContext)
	if !ok || rc == nil {
		return nil, ErrNoRequestContext
	}
	return rc, nil
}

// TenantID returns the tenant carried by ctx, or "" when absent.
//
// Use this only for logging and metrics, where a missing tenant should not
// abort the operation. Anything that selects configuration or data must use
// FromContext and fail closed on error, because silently treating an unknown
// tenant as empty is how cross-tenant leaks happen.
func TenantID(ctx context.Context) string {
	rc, err := FromContext(ctx)
	if err != nil {
		return ""
	}
	return rc.TenantID
}

// TraceID returns the trace id carried by ctx, or "" when absent.
func TraceID(ctx context.Context) string {
	rc, err := FromContext(ctx)
	if err != nil {
		return ""
	}
	return rc.TraceID
}

// LogFields returns the identity fields in a form suitable for structured
// logging. Keeping this in one place stops each call site from inventing its
// own key names, which would make logs unjoinable across processes.
func (rc *RequestContext) LogFields() map[string]string {
	if rc == nil {
		return nil
	}
	return map[string]string{
		"tenant_id":     rc.TenantID,
		"agent_app_id":  rc.AgentAppID,
		"agent_version": rc.AgentVersion,
		"channel":       rc.Channel,
		"session_id":    rc.SessionID,
		"request_id":    rc.RequestID,
		"trace_id":      rc.TraceID,
	}
}

// RuntimeKey returns the key identifying which Runtime should serve this
// request. It always includes TenantID: two tenants may use the same agent
// name and version number, and a cache key without the tenant would let them
// share models, tools, knowledge bases and credentials.
func (rc *RequestContext) RuntimeKey() RuntimeKey {
	return RuntimeKey{
		TenantID:     rc.TenantID,
		AgentAppID:   rc.AgentAppID,
		AgentVersion: rc.AgentVersion,
	}
}
