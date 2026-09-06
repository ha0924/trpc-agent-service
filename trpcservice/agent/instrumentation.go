// 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」
//                docs/治理监控与安全设计.md 监控指标

package agent

import (
	"context"
	"fmt"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// instrumentation records model and tool timings.
//
// It exists as an extension rather than as code inside the assembler because
// the timings have to be taken *inside* the framework's callbacks — that is
// the only place where the start and end of a model call are both observable.
// The Worker sees one agent run, not the individual calls within it.
//
// Registered as a normal extension so a version can turn it off, but it is
// enabled for every seeded version: an agent whose model latency is not
// measured cannot be diagnosed when it gets slow.
func instrumentation(binding types.ExtensionBinding, mp *MountPoints) error {
	if mp.Deps.Metrics == nil {
		// Without a recorder there is nothing to feed. Refusing to mount is
		// better than mounting hooks that measure and discard.
		return fmt.Errorf("instrumentation needs a metrics recorder")
	}
	if mp.Model == nil || mp.Tool == nil {
		return fmt.Errorf("instrumentation needs model and tool mount points")
	}

	modelName := ""
	if mp.Spec != nil {
		modelName = mp.Spec.ModelName
	}

	// Start times are keyed by the request/args pointer the framework hands
	// to both callbacks. A context value would not survive: the framework may
	// pass the after-callback a derived context.
	//
	// A map rather than a single field because parallel tool calls are
	// allowed, so several calls can be in flight at once.
	var (
		mu          sync.Mutex
		modelStarts = make(map[*model.Request]time.Time)
		toolStarts  = make(map[string]time.Time)
	)

	mp.Model.RegisterBeforeModel(func(ctx context.Context, args *model.BeforeModelArgs) (*model.BeforeModelResult, error) {
		mu.Lock()
		modelStarts[args.Request] = time.Now()
		mu.Unlock()
		return nil, nil
	})

	mp.Model.RegisterAfterModel(func(ctx context.Context, args *model.AfterModelArgs) (*model.AfterModelResult, error) {
		mu.Lock()
		start, ok := modelStarts[args.Request]
		delete(modelStarts, args.Request)
		mu.Unlock()
		if !ok {
			// No matching before-callback. Recording a bogus duration would
			// be worse than recording nothing.
			return nil, nil
		}
		mp.Deps.Metrics.ModelCall(ctx, modelName, start, args.Error)
		return nil, nil
	})

	mp.Tool.RegisterBeforeTool(func(ctx context.Context, args *tool.BeforeToolArgs) (*tool.BeforeToolResult, error) {
		mu.Lock()
		toolStarts[args.ToolCallID] = time.Now()
		mu.Unlock()
		return nil, nil
	})

	mp.Tool.RegisterAfterTool(func(ctx context.Context, args *tool.AfterToolArgs) (*tool.AfterToolResult, error) {
		mu.Lock()
		start, ok := toolStarts[args.ToolCallID]
		delete(toolStarts, args.ToolCallID)
		mu.Unlock()
		if !ok {
			return nil, nil
		}

		// A call the whitelist refused never reached the tool, so timing it
		// as a failure would inflate the error rate with policy decisions.
		// It is counted separately instead.
		if denied(args.Result) {
			mp.Deps.Metrics.ToolDenied(ctx, args.ToolName)
			return nil, nil
		}
		mp.Deps.Metrics.ToolCall(ctx, args.ToolName, start, args.Error)
		return nil, nil
	})

	return nil
}

// denied reports whether a tool result is a policy refusal rather than a real
// execution.
//
// The whitelist returns a CustomResult instead of running the tool, so from
// the framework's point of view the call "succeeded". Distinguishing the two
// keeps refusals out of the latency histogram and out of the error rate.
func denied(result any) bool {
	m, ok := result.(map[string]any)
	if !ok {
		return false
	}
	if e, ok := m["error"].(string); ok && e == "permission_denied" {
		return true
	}
	if s, ok := m["status"].(string); ok && s == "awaiting_approval" {
		return true
	}
	return false
}
