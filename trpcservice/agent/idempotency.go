// 设计依据：docs/风险清单.md #2「有副作用 Tool 被重复执行」
//                docs/治理监控与安全设计.md §7.2「治理策略」

package agent

import (
	"context"
	"encoding/json"

	"trpc.group/trpc-go/trpc-agent-go/tool"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// operationIdempotency injects an operation key into calls to tools that
// declare side effects.
//
// This closes a gap between the design and the code. 风险清单 #2 rates
// "有副作用 Tool 被重复执行" as a high risk and names the operation key as its
// first mitigation; everything else in that row was implemented — audit before
// effect, no blind retry on uncertain results, uk_event at the front door —
// but the key itself existed only in prose.
//
// The gap mattered because the platform is at-least-once by design. Queue
// redelivery, sweeper requeues, Worker failover and IM retries all mean one
// request can reach a tool more than once. For a read that is harmless; for a
// refund it is not.
//
// Which tools get a key is configuration, not a guess:
//
//	agent_tool_bindings.params: {"side_effect": true}
//
// Opt-in rather than opt-out. Injecting an unexpected field into a tool that
// does not declare one would make the model's arguments not match the tool's
// schema, and the call would fail — turning a safety feature into an outage.
// A tool that has side effects and does not say so stays unprotected, which
// is visible in its binding row rather than hidden in code.
func operationIdempotency(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Tool == nil {
		return errNeedsToolMountPoints("operation_idempotency")
	}

	sideEffect := make(map[string]bool)
	if mp.Spec != nil {
		for _, tb := range mp.Spec.Tools {
			if boolParam(tb.Params, "side_effect", false) {
				sideEffect[tb.ToolName] = true
			}
		}
	}
	if len(sideEffect) == 0 {
		// No tool declares side effects, so adding a hook would cost a
		// callback per call to do nothing.
		return nil
	}

	mp.Logger.Info("operation idempotency armed",
		"tools", len(sideEffect))

	mp.Tool.RegisterBeforeTool(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		if !sideEffect[args.ToolName] {
			return nil, nil
		}

		rc, _ := types.FromContext(ctx)
		log := loggerFor(ctx, mp.Logger)

		key := types.OperationKey{
			RequestID:       requestOf(rc),
			ToolName:        args.ToolName,
			Sequence:        types.OperationCounterFrom(ctx).Next(),
			ArgsFingerprint: types.FingerprintArgs(args.Arguments),
		}
		if !key.Valid() {
			// No request id means nothing stable to key on, so any generated
			// value would differ between retries and deduplicate against
			// nothing. Refusing is the safe reading: a side effect without a
			// key is exactly what this guardrail exists to prevent, and the
			// cost of refusing is a failed turn rather than a double refund.
			log.Error("side-effect tool called without a request id, refusing",
				"tool", args.ToolName)
			auditDecision(ctx, mp, types.AuditToolCall, args.ToolName,
				types.DecisionDeny, "no request id to derive an operation key from")
			return &tool.BeforeToolResult{
				CustomResult: map[string]any{
					"error":  "no_operation_key",
					"detail": "本次调用缺少可用于幂等的请求标识，已拒绝执行。",
				},
			}, nil
		}

		injected, err := injectOperationKey(args.Arguments, key.String())
		if err != nil {
			// The arguments could not be parsed, so the key cannot be added.
			// Proceeding would run an unprotected side effect; refusing costs
			// one failed turn.
			log.Error("injecting operation key failed, refusing the call",
				"tool", args.ToolName, "error", applog.Scrub(err.Error()))
			auditDecision(ctx, mp, types.AuditToolCall, args.ToolName,
				types.DecisionDeny, "could not inject an operation key")
			return &tool.BeforeToolResult{
				CustomResult: map[string]any{
					"error":  "no_operation_key",
					"detail": "无法为本次调用注入幂等键，已拒绝执行。",
				},
			}, nil
		}

		// Arguments are mutable by contract (see tool.BeforeToolArgs), which
		// is what allows the key to reach the tool without every tool having
		// to ask for it.
		args.Arguments = injected

		// Recorded so a duplicate effect can be traced back to whether the
		// key was actually supplied. Without this, "did the platform send a
		// key" would be unanswerable after the fact.
		auditDecision(ctx, mp, types.AuditToolCall, args.ToolName,
			types.DecisionAllow, "operation key issued: "+key.String())

		log.Info("operation key issued",
			"tool", args.ToolName, "operation_key", key.String(),
			"sequence", key.Sequence)

		return nil, nil
	})
	return nil
}

// injectOperationKey adds the key to a JSON argument object.
//
// Decoded and re-encoded rather than string-spliced: the arguments come from a
// model and may be formatted any way at all, and a textual insert would break
// on the first unusual whitespace or escape.
//
// An existing value is overwritten. A model that invented its own key would
// otherwise defeat deduplication by varying it between retries — the key must
// come from the platform, which is the only party that knows a call is a
// repeat.
func injectOperationKey(raw []byte, key string) ([]byte, error) {
	fields := map[string]any{}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &fields); err != nil {
			return nil, err
		}
	}
	fields[types.OperationKeyField()] = key
	return json.Marshal(fields)
}
