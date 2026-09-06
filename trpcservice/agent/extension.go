// 设计依据：docs/技术设计方案.md §7.1「Plugin、Guardrail 和 Callback」
//                docs/dev/一期实现内容.md §4「结构性约束」

package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// MountPoints collects the framework hooks an extension may attach to.
//
// Extensions receive this rather than the agent, so an extension cannot reach
// past its mount point and reconfigure the agent itself.
//
// Deps carries the collaborators a policy needs — the audit sink, the budget
// counter, the store. Passing them here rather than letting each extension
// construct its own keeps credentials and connections in one place.
type MountPoints struct {
	Agent  *agent.Callbacks
	Model  *model.Callbacks
	Tool   *tool.Callbacks
	Logger *slog.Logger

	// Spec is the version being assembled. Policies read it to know which
	// tools are bound and in what mode.
	Spec *types.RuntimeSpec

	// Deps are the platform services a policy may use. Any of them may be
	// nil when a process does not provide it; policies must degrade rather
	// than panic.
	Deps PolicyDeps
}

// PolicyDeps are the platform services governance policies depend on.
type PolicyDeps struct {
	Audit   types.AuditSink
	Budget  BudgetCounter
	Tenants TenantSettingsLoader
	Users   ChannelUserLoader
	// Approvals persists and spends dangerous-tool confirmations. Nil turns
	// the approval guardrail into an unconditional refusal — safe, but the
	// tool becomes unusable, so the assembler logs it.
	Approvals ApprovalStore
	// Metrics receives model and tool timings. Nil disables that recording
	// rather than failing assembly: instrumentation must never be the reason
	// an agent cannot be built.
	Metrics MetricsRecorder
}

// ApprovalStore is the confirmation half of the dangerous-tool guardrail.
//
// Two methods rather than one because they answer at different moments: the
// claim happens on the *next* call after a human decided, not during the call
// that triggered the request. A dangerous tool call therefore always takes two
// turns — the first records intent and refuses, the second spends the approval.
type ApprovalStore interface {
	// CreateToolApproval records a pending request. It must return only after
	// the row is durable: a crash between intent and effect has to leave
	// evidence of what was attempted.
	CreateToolApproval(ctx context.Context, a *types.ToolApproval) error

	// ClaimToolApproval atomically spends one approved request, reporting
	// whether it won. Atomicity is what stops two concurrent calls from both
	// seeing "approved" and both running the tool on a single approval.
	ClaimToolApproval(ctx context.Context, tenantID, sessionID, toolName, fingerprint string) (bool, error)
}

// MetricsRecorder receives the timings taken inside framework callbacks.
//
// An interface rather than the concrete recorder so the agent package does not
// depend on the metrics package, and so a test can count calls.
type MetricsRecorder interface {
	ModelCall(ctx context.Context, model string, start time.Time, err error)
	ToolCall(ctx context.Context, tool string, start time.Time, err error)
	ToolDenied(ctx context.Context, tool string)
}

// BudgetCounter tracks token consumption per tenant and period.
//
// An interface rather than the concrete Redis type so the agent package does
// not depend on the scheduler, and so a test can supply a fake.
type BudgetCounter interface {
	UsedTokens(ctx context.Context, tenantID string, period string) (int64, error)
	AddTokens(ctx context.Context, tenantID string, period string, tokens int64) (int64, error)
}

// TenantSettingsLoader reads a tenant's policy knobs.
type TenantSettingsLoader interface {
	TenantSettings(ctx context.Context, tenantID string) (*types.TenantSettings, error)
}

// ChannelUserLoader resolves an external IM identity and its attributes.
type ChannelUserLoader interface {
	ChannelUserByExternalID(ctx context.Context, bindingID, externalUserID string) (*types.ChannelUser, error)
}

// Extension attaches one configured capability to the mount points.
//
// The binding carries the version's parameters, so the same implementation can
// behave differently per agent version without a separate registration.
type Extension func(binding types.ExtensionBinding, mp *MountPoints) error

// ExtensionRegistry maps extension names to implementations.
//
// Implementations live in code and are referenced by name from
// agent_extension_bindings. The database records which are enabled, their
// order and their parameters — never the implementation. Adding a guardrail in
// a later phase is therefore a registration plus a row, with no change to the
// assembly path and no schema migration.
type ExtensionRegistry struct {
	mu   sync.RWMutex
	exts map[string]Extension
}

// NewExtensionRegistry returns a registry preloaded with the built-ins.
//
// The five governance policies required of the platform are all here, each
// attached to a framework mount point rather than to a bespoke hook:
//
//	instrumentation           BeforeModel, AfterModel, BeforeTool, AfterTool
//	tool_whitelist            BeforeTool   allow / deny / ask per version
//	dangerous_tool_approval   BeforeTool   confirmation before side effects
//	redaction                 BeforeModel, AfterModel, AfterTool
//	budget_limit              BeforeModel, AfterModel
//	user_permission           BeforeAgent
//
// Which of them a version actually runs, and in what order, comes from
// agent_extension_bindings — none of this is hardcoded into assembly.
func NewExtensionRegistry() *ExtensionRegistry {
	r := &ExtensionRegistry{exts: make(map[string]Extension)}
	r.Register("request_logger", requestLogger)
	r.Register("instrumentation", instrumentation)
	r.Register("tool_whitelist", toolWhitelist)
	r.Register("dangerous_tool_approval", dangerousToolApproval)
	r.Register("redaction", redaction)
	r.Register("budget_limit", budgetLimit)
	r.Register("user_permission", userPermission)
	// Injects an operation key into tools that declare side effects, so a
	// redelivered request cannot cause the effect twice. See 风险清单 #2.
	r.Register("operation_idempotency", operationIdempotency)
	return r
}

// Register adds or replaces an extension implementation.
func (r *ExtensionRegistry) Register(name string, e Extension) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.exts[name] = e
}

// Names lists registered extensions, sorted for stable logging.
func (r *ExtensionRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.exts))
	for n := range r.exts {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Mount attaches every enabled binding, in priority order.
//
// Iteration order is load-bearing, not cosmetic: redaction has to run before
// audit logging, so mount order is configured through priority rather than
// left to whatever order rows came back in.
//
// A binding naming an unregistered extension is an error. A published version
// that expected a guardrail must not quietly run without it.
func (r *ExtensionRegistry) Mount(bindings []types.ExtensionBinding, mp *MountPoints) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ordered := make([]types.ExtensionBinding, 0, len(bindings))
	for _, b := range bindings {
		if b.Enabled {
			ordered = append(ordered, b)
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Kind != ordered[j].Kind {
			return ordered[i].Kind < ordered[j].Kind
		}
		return ordered[i].Priority < ordered[j].Priority
	})

	for _, b := range ordered {
		e, ok := r.exts[b.ExtensionName]
		if !ok {
			return fmt.Errorf("extension %q (%s) is bound but not registered", b.ExtensionName, b.Kind)
		}
		if err := e(b, mp); err != nil {
			return fmt.Errorf("mount extension %q: %w", b.ExtensionName, err)
		}
	}
	return nil
}

// requestLogger records model calls with the tenant context attached.
//
// It is the one built-in extension in phase one. Its value is less the logging
// itself than proving the mechanism: the list is walked and mounted from
// configuration, so a second extension is a data change.
func requestLogger(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Model == nil {
		return fmt.Errorf("request_logger needs model mount points")
	}

	// Start time travels between the two callbacks through this closure
	// rather than through the context, because the framework may hand the
	// after-callback a derived context.
	var mu sync.Mutex
	started := make(map[*model.Request]time.Time)

	mp.Model.RegisterBeforeModel(func(ctx context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		mu.Lock()
		started[args.Request] = time.Now()
		mu.Unlock()

		log := loggerFor(ctx, mp.Logger)
		log.Debug("model call starting", "messages", len(args.Request.Messages))
		return nil, nil
	})

	mp.Model.RegisterAfterModel(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		mu.Lock()
		start, ok := started[args.Request]
		delete(started, args.Request)
		mu.Unlock()

		log := loggerFor(ctx, mp.Logger)
		attrs := []any{}
		if ok {
			attrs = append(attrs, "latency_ms", time.Since(start).Milliseconds())
		}
		if args.Error != nil {
			log.Error("model call failed", append(attrs, "error", args.Error.Error())...)
			return nil, nil
		}
		if args.Response != nil && args.Response.Usage != nil {
			attrs = append(attrs,
				"prompt_tokens", args.Response.Usage.PromptTokens,
				"completion_tokens", args.Response.Usage.CompletionTokens,
				"total_tokens", args.Response.Usage.TotalTokens)
		}
		log.Info("model call finished", attrs...)
		return nil, nil
	})
	return nil
}

// loggerFor attaches the request identity when the context carries one.
func loggerFor(ctx context.Context, base *slog.Logger) *slog.Logger {
	if base == nil {
		base = slog.Default()
	}
	rc, err := types.FromContext(ctx)
	if err != nil {
		return base
	}
	return base.With(
		"tenant_id", rc.TenantID,
		"agent_version", rc.AgentVersion,
		"session_id", rc.SessionID,
		"trace_id", rc.TraceID,
	)
}
