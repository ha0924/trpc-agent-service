// 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」

package metrics

import (
	"context"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Recorder is the platform-facing API over a Registry.
//
// Call sites use these named methods rather than raw metric strings, so a
// typo cannot silently create a parallel series that nobody graphs, and so
// each indicator has exactly one definition of which labels it carries.
type Recorder struct {
	reg *Registry
}

// NewRecorder wraps a registry.
func NewRecorder(reg *Registry) *Recorder {
	if reg == nil {
		reg = NewRegistry()
	}
	return &Recorder{reg: reg}
}

// Registry exposes the underlying registry, for the /metrics handler.
func (r *Recorder) Registry() *Registry { return r.reg }

// labelsFrom builds labels from whatever identity the context carries.
//
// Session and request ids are deliberately absent: they are unbounded and
// would turn every conversation into a new time series.
func labelsFrom(ctx context.Context) Labels {
	rc, err := types.FromContext(ctx)
	if err != nil {
		return Labels{}
	}
	return Labels{
		Tenant:  rc.TenantID,
		Agent:   rc.AgentAppID,
		Channel: rc.Channel,
	}
}

// InboundReceived counts a message accepted at the gateway.
func (r *Recorder) InboundReceived(ctx context.Context, channel, tenant string) {
	r.reg.Inc(MetricRequests, Labels{Tenant: tenant, Channel: channel, Outcome: OutcomeSuccess})
}

// InboundRejected counts a message refused before it was queued.
func (r *Recorder) InboundRejected(ctx context.Context, channel, tenant, reason string) {
	r.reg.Inc(MetricRequests, Labels{Tenant: tenant, Channel: channel, Outcome: OutcomeDenied})
	r.reg.Inc(MetricErrors, Labels{Tenant: tenant, Channel: channel, Outcome: reason})
}

// AgentRun records one agent execution and its outcome.
func (r *Recorder) AgentRun(ctx context.Context, start time.Time, err error) {
	l := labelsFrom(ctx)
	l.Outcome = OutcomeSuccess
	if err != nil {
		l.Outcome = OutcomeFailure
		r.reg.Inc(MetricErrors, Labels{Tenant: l.Tenant, Agent: l.Agent, Outcome: "agent_run"})
	}
	r.reg.Inc(MetricRequests, l)
}

// ModelCall records a model call's latency and outcome.
func (r *Recorder) ModelCall(ctx context.Context, model string, start time.Time, err error) {
	l := labelsFrom(ctx)
	l.Model = model
	l.Outcome = OutcomeSuccess
	if err != nil {
		l.Outcome = OutcomeFailure
		r.reg.Inc(MetricErrors, Labels{Tenant: l.Tenant, Model: model, Outcome: "model_call"})
	}
	r.reg.ObserveSince(MetricModelLatency, l, start)
}

// ToolCall records a tool call's latency and outcome.
func (r *Recorder) ToolCall(ctx context.Context, tool string, start time.Time, err error) {
	l := labelsFrom(ctx)
	l.Tool = tool
	l.Outcome = OutcomeSuccess
	if err != nil {
		l.Outcome = OutcomeFailure
		r.reg.Inc(MetricErrors, Labels{Tenant: l.Tenant, Tool: tool, Outcome: "tool_call"})
	}
	r.reg.ObserveSince(MetricToolLatency, l, start)
}

// ToolDenied counts a tool call refused by policy. Kept apart from failures:
// a denial is policy working, not a fault, and mixing them would make the
// error rate alarm on correct behaviour.
func (r *Recorder) ToolDenied(ctx context.Context, tool string) {
	l := labelsFrom(ctx)
	l.Tool = tool
	l.Outcome = OutcomeDenied
	r.reg.Inc(MetricRequests, l)
}

// Delivery records an outbound reply attempt. Success and failure share one
// metric so a delivery success rate is a ratio of the same series.
func (r *Recorder) Delivery(ctx context.Context, channel string, err error) {
	l := labelsFrom(ctx)
	l.Channel = channel
	l.Outcome = OutcomeSuccess
	if err != nil {
		l.Outcome = OutcomeFailure
		r.reg.Inc(MetricErrors, Labels{Tenant: l.Tenant, Channel: channel, Outcome: "delivery"})
	}
	r.reg.Inc(MetricDelivery, l)
}

// Tokens records consumption and cost for one model call.
//
// Cost is recorded alongside tokens rather than derived at query time so a
// later change in pricing does not silently rewrite historical spend.
func (r *Recorder) Tokens(ctx context.Context, model string, prompt, completion int, costUSD float64) {
	l := labelsFrom(ctx)
	l.Model = model

	r.reg.Add(MetricTokens, Labels{Tenant: l.Tenant, Agent: l.Agent, Model: model, Outcome: "prompt"}, float64(prompt))
	r.reg.Add(MetricTokens, Labels{Tenant: l.Tenant, Agent: l.Agent, Model: model, Outcome: "completion"}, float64(completion))
	if costUSD > 0 {
		r.reg.Add(MetricCost, Labels{Tenant: l.Tenant, Agent: l.Agent, Model: model}, costUSD)
	}
}

// StorageCall records a session backend operation's latency.
func (r *Recorder) StorageCall(ctx context.Context, backend, op string, start time.Time, err error) {
	l := Labels{Tenant: types.TenantID(ctx), Model: backend, Tool: op, Outcome: OutcomeSuccess}
	if err != nil {
		l.Outcome = OutcomeFailure
	}
	r.reg.ObserveSince(MetricStorageLatency, l, start)
}

// QueueDepth reports pending hints.
func (r *Recorder) QueueDepth(n int64) {
	r.reg.Set(MetricQueueDepth, Labels{}, float64(n))
}

// RuntimeCached reports how many Runtimes are held.
func (r *Recorder) RuntimeCached(n int) {
	r.reg.Set(MetricRuntimeCached, Labels{}, float64(n))
}

// LeaseContention counts a Worker losing a race for a session lease.
//
// Not an error: under at-least-once delivery several Workers routinely see
// the same hint. It is tracked because a sudden rise means either duplicate
// hints or a session that is monopolising a Worker.
func (r *Recorder) LeaseContention(tenant string) {
	r.reg.Inc(MetricLeaseContention, Labels{Tenant: tenant})
}

// AuditDropped reports records discarded because the audit buffer was full.
// Any non-zero value means the audit trail has gaps and the buffer needs
// resizing.
func (r *Recorder) AuditDropped(n int64) {
	r.reg.Set(MetricAuditDropped, Labels{}, float64(n))
}
