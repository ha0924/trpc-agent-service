// 设计依据：docs/多租户与节点部署设计.md §2.3「版本承载的配置」
//                docs/技术设计方案.md §7.2「治理策略」

// Package tool registers the platform's built-in tools and resolves the tool
// list a given agent version is allowed to use.
//
// Tools are registered in code and referenced by name from
// agent_tool_bindings. The database stores which tools a version may call and
// under what mode, never the implementation, so adding a tool is a
// registration plus a binding row — no schema change and no change to the
// assembly path.
package tool

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"trpc.group/trpc-go/trpc-agent-go/tool"
	"trpc.group/trpc-go/trpc-agent-go/tool/function"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Factory builds a tool instance for one agent version.
//
// A factory rather than a shared instance, because a version may override
// timeouts or limits through agent_tool_bindings.params, and because a tool
// holding tenant-specific credentials must not be shared across tenants.
type Factory func(params map[string]any) (tool.Tool, error)

// Registry maps tool names to factories.
type Registry struct {
	mu        sync.RWMutex
	factories map[string]Factory
}

// NewRegistry returns a registry preloaded with the platform tools.
func NewRegistry() *Registry {
	r := &Registry{factories: make(map[string]Factory)}
	r.Register("calculator", newCalculator)
	r.Register("search", newSearch)
	return r
}

// Register adds or replaces a tool factory.
func (r *Registry) Register(name string, f Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.factories[name] = f
}

// Names lists registered tools, sorted for stable logging.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.factories))
	for n := range r.factories {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Resolve turns a version's tool bindings into tool instances.
//
// Modes are honoured here rather than at call time for deny: a denied tool is
// simply never given to the agent, so the model cannot even see it exists.
// "ask" tools are included but must be gated by a guardrail before the call
// takes effect — that gate is a phase-three concern, and the audit record for
// it has to be written before the side effect, not after.
//
// An unknown tool name is an error rather than a silent skip: a version that
// was published expecting a tool must not quietly run without it.
func (r *Registry) Resolve(bindings []types.ToolBinding) ([]tool.Tool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]tool.Tool, 0, len(bindings))
	for _, b := range bindings {
		if b.Mode == types.ToolModeDeny {
			continue
		}
		f, ok := r.factories[b.ToolName]
		if !ok {
			return nil, fmt.Errorf("tool %q is bound but not registered", b.ToolName)
		}
		t, err := f(b.Params)
		if err != nil {
			return nil, fmt.Errorf("build tool %q: %w", b.ToolName, err)
		}
		out = append(out, t)
	}
	return out, nil
}

// ---------------------------------------------------------------------------
// calculator
// ---------------------------------------------------------------------------

type calcInput struct {
	Operation string  `json:"operation" jsonschema:"description=One of add, subtract, multiply, divide"`
	A         float64 `json:"a" jsonschema:"description=Left operand"`
	B         float64 `json:"b" jsonschema:"description=Right operand"`
}

type calcOutput struct {
	Result float64 `json:"result"`
}

func newCalculator(_ map[string]any) (tool.Tool, error) {
	return function.NewFunctionTool(
		func(ctx context.Context, in calcInput) (calcOutput, error) {
			switch strings.ToLower(strings.TrimSpace(in.Operation)) {
			case "add", "+":
				return calcOutput{Result: in.A + in.B}, nil
			case "subtract", "sub", "-":
				return calcOutput{Result: in.A - in.B}, nil
			case "multiply", "mul", "*":
				return calcOutput{Result: in.A * in.B}, nil
			case "divide", "div", "/":
				if in.B == 0 {
					// Returned as an error so the model sees why it failed and
					// can rephrase, rather than receiving a silent Inf.
					return calcOutput{}, fmt.Errorf("division by zero")
				}
				return calcOutput{Result: in.A / in.B}, nil
			default:
				return calcOutput{}, fmt.Errorf("unsupported operation %q", in.Operation)
			}
		},
		function.WithName("calculator"),
		function.WithDescription("Perform basic arithmetic: add, subtract, multiply or divide two numbers."),
	), nil
}

// ---------------------------------------------------------------------------
// search
// ---------------------------------------------------------------------------

type searchInput struct {
	Query string `json:"query" jsonschema:"description=What to search for"`
}

type searchOutput struct {
	Query   string         `json:"query"`
	Results []searchResult `json:"results"`
	Note    string         `json:"note,omitempty"`
}

type searchResult struct {
	Title   string `json:"title"`
	Snippet string `json:"snippet"`
}

// newSearch builds the search tool.
//
// Phase one returns a canned result rather than calling a real search
// provider. The point of including it is to prove the assembly path handles a
// list of tools and that the tenant context reaches tool execution; wiring a
// real provider later changes this function only.
func newSearch(_ map[string]any) (tool.Tool, error) {
	return function.NewFunctionTool(
		func(ctx context.Context, in searchInput) (searchOutput, error) {
			// The tenant travels with the context all the way into tool
			// execution. A tool that queries business data must filter by it,
			// so proving it arrives here matters more than the result itself.
			tenant := types.TenantID(ctx)

			return searchOutput{
				Query: in.Query,
				Results: []searchResult{{
					Title:   "占位搜索结果",
					Snippet: fmt.Sprintf("针对 %q 的检索尚未接入真实搜索后端。", in.Query),
				}},
				Note: fmt.Sprintf("stub search executed for tenant %q", tenant),
			}, nil
		},
		function.WithName("search"),
		function.WithDescription("Search for information on a topic. Returns a list of results."),
	), nil
}
