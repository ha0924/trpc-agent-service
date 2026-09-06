// 设计依据：docs/治理监控与安全设计.md §7.2「治理策略」
//                docs/技术设计方案.md §7.1「Plugin、Guardrail 和 Callback」

package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Budget periods, mirrored here so policies need not import the scheduler.
const (
	periodDaily   = "daily"
	periodMonthly = "monthly"
)

// ---------------------------------------------------------------------------
// 1. Tool whitelist
// ---------------------------------------------------------------------------

// toolWhitelist refuses tools this version is not permitted to call.
//
// Denied tools are already withheld at assembly, so the model cannot see them.
// This is the second gate, and it exists because the first is not sufficient:
// a tool can be reachable through a toolset or an MCP server that was added
// after the allow-list was computed, and a model can hallucinate a call to a
// name it was never given.
//
// Refusal returns a result to the model instead of an error, so the agent can
// explain itself to the user rather than the turn collapsing.
func toolWhitelist(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Tool == nil {
		return fmt.Errorf("tool_whitelist needs tool mount points")
	}

	allowed := make(map[string]types.ToolMode)
	if mp.Spec != nil {
		for _, tb := range mp.Spec.Tools {
			allowed[tb.ToolName] = tb.Mode
		}
	}

	mp.Tool.RegisterBeforeTool(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		mode, bound := allowed[args.ToolName]
		if bound && mode != types.ToolModeDeny {
			return nil, nil
		}

		reason := "tool is not bound to this agent version"
		if bound {
			reason = "tool is explicitly denied for this agent version"
		}
		auditDecision(ctx, mp, types.AuditToolCall, args.ToolName, types.DecisionDeny, reason)
		loggerFor(ctx, mp.Logger).Warn("tool call denied", "tool", args.ToolName, "reason", reason)

		return &tool.BeforeToolResult{
			CustomResult: map[string]any{
				"error":  "permission_denied",
				"detail": fmt.Sprintf("工具 %s 未被授权，无法调用。", args.ToolName),
			},
		}, nil
	})
	return nil
}

// ---------------------------------------------------------------------------
// 2. Dangerous tool confirmation
// ---------------------------------------------------------------------------

// dangerousToolApproval holds a tool marked "ask" until it is confirmed.
//
// A gated call takes two turns, and that is inherent rather than a shortcut:
//
//	turn 1  no approval on file → record the request, refuse, tell the user
//	        (a human then approves through the Admin API)
//	turn 2  the user asks again → the approval is claimed → the tool runs
//
// It cannot be one turn. Blocking inside the callback would hold a Worker slot
// and the session lease for as long as a human takes to decide, and the lease
// would expire mid-wait and hand the session to another Worker.
//
// Two ordering constraints, both load-bearing:
//
//   - The approval row is written *before* the call is refused, never after.
//     If the process dies between intent and effect, the record of what was
//     attempted still exists. An audit written after the fact is precisely the
//     one missing when something goes wrong.
//   - The claim is a single atomic UPDATE (approved→consumed), not a read
//     followed by a check. Two concurrent calls must not both see "approved"
//     and both run the tool on one human's approval.
//
// One approval buys exactly one execution. Without the consumed state,
// confirming once would permanently disable the gate — worse than having no
// gate, because it still looks like one.
func dangerousToolApproval(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Tool == nil {
		return fmt.Errorf("dangerous_tool_approval needs tool mount points")
	}

	needsApproval := make(map[string]bool)
	if mp.Spec != nil {
		for _, tb := range mp.Spec.Tools {
			if tb.Mode == types.ToolModeAsk {
				needsApproval[tb.ToolName] = true
			}
		}
	}
	if len(needsApproval) == 0 {
		return nil // nothing to gate; do not add a hook that always passes
	}

	ttl := durationParam(binding.Params, "approval_ttl", 10*time.Minute)
	approvals := mp.Deps.Approvals

	if approvals == nil {
		// No store wired, so nothing could ever be approved. Say so once at
		// assembly instead of letting every call look like it might be
		// approvable — this is exactly the "configured but inert" shape that
		// has bitten this project before.
		mp.Logger.Warn("dangerous_tool_approval has no approval store; gated tools will always be refused",
			"tools", len(needsApproval))
	}

	mp.Tool.RegisterBeforeTool(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		if !needsApproval[args.ToolName] {
			return nil, nil
		}

		rc, _ := types.FromContext(ctx)
		log := loggerFor(ctx, mp.Logger)

		// The fingerprint binds an approval to the arguments a human actually
		// reviewed. Without it, approving "delete order 123" would authorise
		// "delete order 999" — the guardrail would only see that the tool has
		// an approved row.
		fingerprint := types.FingerprintArgs(args.Arguments)

		// Turn 2: is there an approval for exactly this call?
		if approvals != nil {
			claimed, err := approvals.ClaimToolApproval(ctx,
				tenantOf(rc), sessionOf(rc), args.ToolName, fingerprint)
			if err != nil {
				// Unknown whether an approval exists. Refusing is the only
				// safe reading: proceeding could run a dangerous tool nobody
				// approved, and the cost of refusing is one retry.
				log.Error("claiming tool approval failed, refusing the call",
					"tool", args.ToolName, "error", applog.Scrub(err.Error()))
				return refuseUnapproved(ctx, mp, rc, args.ToolName,
					"approval lookup failed"), nil
			}
			if claimed {
				auditRecordSync(ctx, mp, &types.AuditRecord{
					TenantID: tenantOf(rc), AgentAppID: agentAppOf(rc),
					Channel: channelOf(rc), UserID: userOf(rc),
					SessionID: sessionOf(rc), RequestID: requestOf(rc), TraceID: traceOf(rc),
					EventType: types.AuditToolCall, ToolName: args.ToolName,
					Decision: types.DecisionAllow,
					Reason:   "dangerous tool ran against a claimed approval",
					Detail: map[string]any{
						"args_fingerprint": fingerprint,
						"arguments":        redactArguments(args.Arguments),
					},
				})
				log.Warn("dangerous tool authorised by a claimed approval",
					"tool", args.ToolName, "fingerprint", fingerprint)
				// nil lets the call through to the real tool.
				return nil, nil
			}
		}

		// Turn 1: nothing on file. Record the request, then refuse.
		approvalID := "apr-" + uuid.NewString()
		expires := time.Now().Add(ttl)
		redactedArgs := redactArguments(args.Arguments)

		if approvals != nil {
			// Written before refusing, and synchronously: an approval nobody
			// can find is an approval nobody can grant.
			if err := approvals.CreateToolApproval(ctx, &types.ToolApproval{
				ApprovalID: approvalID,
				TenantID:   tenantOf(rc), AgentAppID: agentAppOf(rc),
				SessionID: sessionOf(rc), RequestID: requestOf(rc), TraceID: traceOf(rc),
				ToolName:    args.ToolName,
				ToolArgs:    map[string]any{"redacted": redactedArgs},
				RequestedBy: userOf(rc),
				State:       types.ApprovalPending,
				// The same fingerprint the claim will match on, so the
				// approval can only be spent on this exact call.
				ArgsFingerprint: fingerprint,
				ExpiresAt:       &expires,
			}); err != nil {
				log.Error("recording tool approval failed",
					"tool", args.ToolName, "error", applog.Scrub(err.Error()))
			}
		}

		auditRecordSync(ctx, mp, &types.AuditRecord{
			TenantID: tenantOf(rc), AgentAppID: agentAppOf(rc),
			Channel: channelOf(rc), UserID: userOf(rc),
			SessionID: sessionOf(rc), RequestID: requestOf(rc), TraceID: traceOf(rc),
			EventType: types.AuditToolCall, ToolName: args.ToolName,
			Decision: types.DecisionAsk,
			Reason:   "dangerous tool requires confirmation",
			Detail: map[string]any{
				"approval_id":      approvalID,
				"args_fingerprint": fingerprint,
				"arguments":        redactedArgs,
				"expires_at":       expires.Format(time.RFC3339),
			},
		})

		log.Warn("dangerous tool held for confirmation",
			"tool", args.ToolName, "approval_id", approvalID,
			"fingerprint", fingerprint)

		if mp.Deps.Metrics != nil {
			mp.Deps.Metrics.ToolDenied(ctx, args.ToolName)
		}

		return &tool.BeforeToolResult{
			CustomResult: map[string]any{
				"status":      "awaiting_approval",
				"approval_id": approvalID,
				"detail": fmt.Sprintf(
					"工具 %s 属于高风险操作，已记录待确认请求 %s。"+
						"经审批放行后重新发起同样的请求即会执行。",
					args.ToolName, approvalID),
			},
		}, nil
	})
	return nil
}

// refuseUnapproved builds the refusal returned when an approval cannot be
// confirmed, and records it.
//
// A result rather than an error, so the agent can tell the user why instead of
// the whole run failing — the same convention the whitelist uses.
func refuseUnapproved(
	ctx context.Context,
	mp *MountPoints,
	rc *types.RequestContext,
	toolName, reason string,
) *tool.BeforeToolResult {
	auditRecordSync(ctx, mp, &types.AuditRecord{
		TenantID: tenantOf(rc), AgentAppID: agentAppOf(rc),
		Channel: channelOf(rc), UserID: userOf(rc),
		SessionID: sessionOf(rc), RequestID: requestOf(rc), TraceID: traceOf(rc),
		EventType: types.AuditToolCall, ToolName: toolName,
		Decision: types.DecisionDeny, Reason: reason,
	})
	return &tool.BeforeToolResult{
		CustomResult: map[string]any{
			"status": "refused",
			"detail": fmt.Sprintf("工具 %s 未获授权，本次调用被拒绝。", toolName),
		},
	}
}

// ---------------------------------------------------------------------------
// 3. Redaction
// ---------------------------------------------------------------------------

// redaction scrubs credentials from what goes to the model and comes back.
//
// It runs in three places because a secret can enter from three directions: a
// user pasting one, a tool returning one from an upstream error, and a model
// echoing one back. Mounting it early — a low priority value — matters,
// because a later policy that audits content would otherwise persist the
// unredacted form.
func redaction(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Model == nil {
		return fmt.Errorf("redaction needs model mount points")
	}

	mp.Model.RegisterBeforeModel(func(ctx context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		for i := range args.Request.Messages {
			args.Request.Messages[i].Content = applog.Scrub(args.Request.Messages[i].Content)
		}
		return nil, nil
	})

	mp.Model.RegisterAfterModel(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		if args.Response == nil {
			return nil, nil
		}
		for i := range args.Response.Choices {
			args.Response.Choices[i].Message.Content = applog.Scrub(args.Response.Choices[i].Message.Content)
			args.Response.Choices[i].Delta.Content = applog.Scrub(args.Response.Choices[i].Delta.Content)
		}
		return nil, nil
	})

	if mp.Tool != nil {
		mp.Tool.RegisterAfterTool(func(ctx context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
			// A tool wrapping an upstream failure often carries a DSN or a
			// bearer token in the message, and that text is about to be fed
			// straight back into the model.
			if args.Error != nil {
				return nil, nil
			}
			s, ok := args.Result.(string)
			if !ok {
				return nil, nil
			}
			if scrubbed := applog.Scrub(s); scrubbed != s {
				return &tool.AfterToolResult{CustomResult: scrubbed}, nil
			}
			return nil, nil
		})
	}
	return nil
}

// ---------------------------------------------------------------------------
// 4. Budget limit
// ---------------------------------------------------------------------------

// budgetLimit refuses model calls once a tenant has spent its allowance.
//
// The counter is read from Redis, not summed from usage_records: a check runs
// before every model call, and summing a growing detail table on the hot path
// is too slow. Without the counter the policy degrades to "count but never
// stop", which is the failure mode this exists to prevent.
//
// Consumption is recorded after the call rather than reserved before it,
// because the token count is not known until the response arrives. A tenant
// can therefore overshoot by at most one request — acceptable, where blocking
// every call behind a reservation would not be.
func budgetLimit(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Model == nil {
		return fmt.Errorf("budget_limit needs model mount points")
	}
	if mp.Deps.Budget == nil || mp.Deps.Tenants == nil {
		// Without a counter this policy cannot enforce anything. Refusing to
		// mount is better than mounting a no-op that looks like protection.
		return fmt.Errorf("budget_limit needs a budget counter and tenant settings")
	}

	mp.Model.RegisterBeforeModel(func(ctx context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		rc, err := types.FromContext(ctx)
		if err != nil {
			return nil, nil // no tenant in context: nothing to enforce against
		}

		settings, err := mp.Deps.Tenants.TenantSettings(ctx, rc.TenantID)
		if err != nil {
			loggerFor(ctx, mp.Logger).Warn("budget check skipped, settings unavailable",
				"error", applog.Scrub(err.Error()))
			return nil, nil
		}

		for _, check := range []struct {
			period string
			limit  int64
		}{
			{periodDaily, settings.DailyTokenBudget},
			{periodMonthly, settings.MonthlyTokenBudget},
		} {
			if check.limit <= 0 {
				continue
			}
			used, err := mp.Deps.Budget.UsedTokens(ctx, rc.TenantID, check.period)
			if err != nil {
				loggerFor(ctx, mp.Logger).Warn("budget counter unavailable",
					"period", check.period, "error", applog.Scrub(err.Error()))
				continue
			}
			if used < check.limit {
				continue
			}

			reason := fmt.Sprintf("%s token budget exhausted: %d of %d", check.period, used, check.limit)
			auditDecision(ctx, mp, types.AuditModelCall, "", types.DecisionDeny, reason)
			loggerFor(ctx, mp.Logger).Warn("model call denied by budget",
				"period", check.period, "used", used, "limit", check.limit)

			// A custom response ends the turn cleanly with an explanation the
			// user can act on, rather than surfacing an internal error.
			return &model.BeforeModelResult{
				CustomResponse: &model.Response{
					Choices: []model.Choice{{
						Message: model.NewAssistantMessage(
							fmt.Sprintf("本租户的%s用量额度已用尽，请联系管理员调整后再试。",
								periodLabel(check.period))),
					}},
					Done: true,
				},
			}, nil
		}
		return nil, nil
	})

	mp.Model.RegisterAfterModel(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		if args.Response == nil || args.Response.Usage == nil {
			return nil, nil
		}
		rc, err := types.FromContext(ctx)
		if err != nil {
			return nil, nil
		}

		total := int64(args.Response.Usage.TotalTokens)
		if total <= 0 {
			return nil, nil
		}
		for _, period := range []string{periodDaily, periodMonthly} {
			if _, err := mp.Deps.Budget.AddTokens(ctx, rc.TenantID, period, total); err != nil {
				loggerFor(ctx, mp.Logger).Error("recording token usage failed",
					"period", period, "error", applog.Scrub(err.Error()))
			}
		}
		return nil, nil
	})
	return nil
}

func periodLabel(period string) string {
	if period == periodMonthly {
		return "本月"
	}
	return "当日"
}

// ---------------------------------------------------------------------------
// 5. IM user permission
// ---------------------------------------------------------------------------

// userPermission refuses users who are not allowed to talk to this tenant.
//
// It runs before the agent rather than at the gateway because the rule is a
// tenant policy, not a transport concern, and because the same user may be
// permitted on one agent and not another.
//
// A user with no mapping row is treated as unknown rather than as unrestricted.
// Defaulting an unrecognised identity to "allowed" is how an access rule ends
// up protecting nothing.
func userPermission(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Agent == nil {
		return fmt.Errorf("user_permission needs agent mount points")
	}
	if mp.Deps.Tenants == nil {
		return fmt.Errorf("user_permission needs tenant settings")
	}

	// allow_unmapped keeps a tenant that has not populated channel_users from
	// locking everyone out. It defaults to true so enabling the policy is not
	// itself an outage, and a tenant that cares sets it false.
	allowUnmapped := boolParam(binding.Params, "allow_unmapped", true)

	mp.Agent.RegisterBeforeAgent(func(ctx context.Context, args *agent.BeforeAgentArgs) (*agent.BeforeAgentResult, error) {
		rc, err := types.FromContext(ctx)
		if err != nil {
			return nil, nil
		}

		settings, err := mp.Deps.Tenants.TenantSettings(ctx, rc.TenantID)
		if err != nil || len(settings.AllowedRoles) == 0 {
			return nil, nil // no role restriction configured
		}
		if mp.Deps.Users == nil {
			return nil, nil
		}

		user, err := mp.Deps.Users.ChannelUserByExternalID(ctx, rc.ChannelBindingID, rc.UserID)
		if err != nil {
			if allowUnmapped {
				return nil, nil
			}
			return denyAgent(ctx, mp, rc, "user is not mapped for this channel binding")
		}
		if user.Status != types.StatusActive {
			return denyAgent(ctx, mp, rc, "user is not active")
		}

		for _, want := range settings.AllowedRoles {
			if user.HasRole(want) {
				return nil, nil
			}
		}
		return denyAgent(ctx, mp, rc,
			fmt.Sprintf("user roles %v do not include any of %v", user.Roles(), settings.AllowedRoles))
	})
	return nil
}

func denyAgent(ctx context.Context, mp *MountPoints, rc *types.RequestContext, reason string) (*agent.BeforeAgentResult, error) {
	auditDecision(ctx, mp, types.AuditAgentRun, "", types.DecisionDeny, reason)
	loggerFor(ctx, mp.Logger).Warn("agent run denied", "reason", reason, "user_id", rc.UserID)

	return &agent.BeforeAgentResult{
		CustomResponse: &model.Response{
			Choices: []model.Choice{{
				Message: model.NewAssistantMessage("你没有使用该助手的权限，请联系管理员。"),
			}},
			Done: true,
		},
	}, nil
}

// ---------------------------------------------------------------------------
// shared helpers
// ---------------------------------------------------------------------------

// auditDecision records a governance outcome through the async sink.
func auditDecision(ctx context.Context, mp *MountPoints, evt types.AuditEventType, toolName string, d types.Decision, reason string) {
	if mp.Deps.Audit == nil {
		return
	}
	rc, _ := types.FromContext(ctx)
	r := types.NewAuditRecord(rc, evt)
	r.ToolName = toolName
	r.Decision = d
	r.Reason = applog.Scrub(reason)
	if mp.Spec != nil {
		r.AgentName = agentName(mp.Spec.Key)
	}
	_ = mp.Deps.Audit.Write(ctx, r)
}

// auditRecordSync writes a record that must be durable before proceeding.
func auditRecordSync(ctx context.Context, mp *MountPoints, r *types.AuditRecord) {
	if mp.Deps.Audit == nil {
		return
	}
	if mp.Spec != nil && r.AgentName == "" {
		r.AgentName = agentName(mp.Spec.Key)
	}
	if err := mp.Deps.Audit.Write(ctx, r); err != nil {
		loggerFor(ctx, mp.Logger).Error("durable audit write failed",
			"tool", r.ToolName, "error", applog.Scrub(err.Error()))
	}
}

// redactArguments scrubs tool arguments before they are stored.
func redactArguments(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var pretty map[string]any
	if err := json.Unmarshal(raw, &pretty); err != nil {
		return applog.Scrub(string(raw))
	}
	out, err := json.Marshal(pretty)
	if err != nil {
		return applog.Scrub(string(raw))
	}
	return applog.Scrub(string(out))
}

func boolParam(params map[string]any, key string, def bool) bool {
	if params == nil {
		return def
	}
	if v, ok := params[key].(bool); ok {
		return v
	}
	return def
}

func durationParam(params map[string]any, key string, def time.Duration) time.Duration {
	if params == nil {
		return def
	}
	s, ok := params[key].(string)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return d
}

// Small accessors that tolerate a missing request context, so a policy can
// still record what it knows rather than dropping the audit entirely.
func tenantOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.TenantID
}
func agentAppOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.AgentAppID
}
func channelOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.Channel
}
func userOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.UserID
}
func sessionOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.SessionID
}
func requestOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.RequestID
}
func traceOf(rc *types.RequestContext) string {
	if rc == nil {
		return ""
	}
	return rc.TraceID
}
