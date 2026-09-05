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
// The order here is the whole point. The approval row is written *before* the
// call is allowed to proceed, never after: if the process dies between intent
// and effect, the record of what was attempted still exists. An audit written
// after the fact is precisely the one missing when something goes wrong.
//
// Phase one records the intent and refuses the call, so a dangerous tool
// cannot run unattended. Wiring an approval channel replaces the refusal with
// a wait; the ordering constraint does not change.
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

	mp.Tool.RegisterBeforeTool(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		if !needsApproval[args.ToolName] {
			return nil, nil
		}

		rc, _ := types.FromContext(ctx)
		approvalID := "apr-" + uuid.NewString()
		expires := time.Now().Add(ttl)

		// Arguments are redacted before they are stored: an approval row is
		// read by more people and kept longer than a log line.
		redactedArgs := redactArguments(args.Arguments)

		// Recorded synchronously and durably. This must not go through the
		// async sink, whose buffer may drop under load.
		auditRecordSync(ctx, mp, &types.AuditRecord{
			TenantID: tenantOf(rc), AgentAppID: agentAppOf(rc),
			Channel: channelOf(rc), UserID: userOf(rc),
			SessionID: sessionOf(rc), RequestID: requestOf(rc), TraceID: traceOf(rc),
			EventType: types.AuditToolCall, ToolName: args.ToolName,
			Decision: types.DecisionAsk,
			Reason:   "dangerous tool requires confirmation",
			Detail: map[string]any{
				"approval_id": approvalID,
				"arguments":   redactedArgs,
				"expires_at":  expires.Format(time.RFC3339),
			},
		})

		loggerFor(ctx, mp.Logger).Warn("dangerous tool held for confirmation",
			"tool", args.ToolName, "approval_id", approvalID)

		return &tool.BeforeToolResult{
			CustomResult: map[string]any{
				"status":      "awaiting_approval",
				"approval_id": approvalID,
				"detail": fmt.Sprintf(
					"工具 %s 属于高风险操作，已记录待确认请求 %s，确认后才会执行。",
					args.ToolName, approvalID),
			},
		}, nil
	})
	return nil
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
