// 设计依据：docs/多租户与节点部署设计.md §6「Runtime 装配与缓存」
//                docs/dev/一期实现内容.md §4「结构性约束」

package types

import (
	"context"
	"fmt"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/runner"
)

// RuntimeKey identifies one assembled agent version.
//
// TenantID is part of the key, not an afterthought. Two tenants may name
// their agents identically and both call a version "v1"; a key without the
// tenant would let them share a Runtime and therefore share models, tools,
// knowledge bases and API credentials. This is the single most consequential
// place where a missing tenant field becomes a cross-tenant leak.
type RuntimeKey struct {
	TenantID     string
	AgentAppID   string
	AgentVersion string
}

// String renders the key for logs and cache maps.
func (k RuntimeKey) String() string {
	return fmt.Sprintf("%s/%s/%s", k.TenantID, k.AgentAppID, k.AgentVersion)
}

// Valid reports whether every component is present. An incomplete key must
// never reach the cache.
func (k RuntimeKey) Valid() bool {
	return k.TenantID != "" && k.AgentAppID != "" && k.AgentVersion != ""
}

// Runtime is an assembled, ready-to-run agent version.
//
// Agent is the framework's agent.Agent interface rather than a concrete
// *llmagent.LLMAgent. Returning the interface means adding graph orchestration
// later is one more branch inside the assembler; returning a concrete type
// would force every caller to change.
type Runtime struct {
	Key    RuntimeKey
	Agent  agent.Agent
	Runner runner.Runner

	// Spec is the configuration this Runtime was built from, kept for
	// diagnostics and for answering "why does this agent behave this way".
	Spec *RuntimeSpec
}

// Close releases the runner and anything it owns.
func (r *Runtime) Close() error {
	if r == nil || r.Runner == nil {
		return nil
	}
	return r.Runner.Close()
}

// RuntimeSpec is the configuration read from the control plane before
// assembly: one row of agent_versions plus the four binding tables.
//
// Assembly is driven by this struct rather than by hardcoded construction.
// The distinction matters because a hardcoded agent cannot be made
// configuration-driven later without rewriting the assembler and everything
// that calls it.
type RuntimeSpec struct {
	Key RuntimeKey

	// From agent_versions.
	SystemPrompt   string
	ModelName      string
	ModelAPIKeyRef string
	ModelParams    map[string]any

	// From the binding tables. Each is a list, and assembly iterates the list
	// rather than mounting known names, so adding a capability in a later
	// phase is a data change, not a code change.
	Tools      []ToolBinding
	Extensions []ExtensionBinding
	MCPServers []MCPBinding
	Skills     []SkillBinding
}

// ToolBinding is one row of agent_tool_bindings.
type ToolBinding struct {
	ToolName string
	// Mode is allow, deny or ask. "ask" means the call needs confirmation
	// before it runs, and the audit record must be written before the side
	// effect, not after.
	Mode   ToolMode
	Params map[string]any
}

// ToolMode is the permission a version has over one tool.
type ToolMode string

const (
	ToolModeAllow ToolMode = "allow"
	ToolModeDeny  ToolMode = "deny"
	ToolModeAsk   ToolMode = "ask"
)

// ExtensionBinding is one row of agent_extension_bindings.
type ExtensionBinding struct {
	// Kind selects which framework mount point this attaches to.
	Kind ExtensionKind
	// ExtensionName matches an implementation registered in code. The
	// implementation itself is never stored in the database, so adding a new
	// extension means registering it, not migrating a table.
	ExtensionName string
	Enabled       bool
	// Priority orders extensions of the same kind, lower first. Order is
	// configurable rather than insertion-dependent because it changes
	// behaviour: redaction has to run before audit logging, not after.
	Priority int
	Params   map[string]any
}

// ExtensionKind names a framework mount point.
type ExtensionKind string

const (
	ExtensionKindPlugin    ExtensionKind = "plugin"
	ExtensionKindGuardrail ExtensionKind = "guardrail"
	ExtensionKindCallback  ExtensionKind = "callback"
)

// MCPBinding is one row of agent_mcp_bindings.
type MCPBinding struct {
	ServerName string
	Enabled    bool
	// ToolFilter exposes only part of an MCP server's tools, since a server
	// typically offers many and a tenant may want a subset.
	ToolFilter []string
}

// SkillBinding is one row of agent_skill_bindings.
type SkillBinding struct {
	SkillName string
	Enabled   bool
	Params    map[string]any
}

// RuntimeProvider assembles and caches Runtimes.
//
// Published versions are immutable, so a cached Runtime never goes stale and
// the only cache concerns are capacity and idle eviction. That immutability
// is also what makes splitting capabilities across four binding tables
// affordable: the extra queries happen once, on first assembly, never on the
// request path.
type RuntimeProvider interface {
	// Get returns the Runtime for key, assembling it on first use.
	// Concurrent calls for the same key must assemble only once.
	Get(ctx context.Context, key RuntimeKey) (*Runtime, error)

	// Invalidate drops a cached Runtime, for version retirement. In-flight
	// requests finish before the Runtime is actually released.
	Invalidate(ctx context.Context, key RuntimeKey) error

	// Close releases every cached Runtime.
	Close() error
}

// SpecLoader reads a RuntimeSpec from the control plane. Separating loading
// from assembly lets the configuration source change without touching how the
// agent is built.
type SpecLoader interface {
	Load(ctx context.Context, key RuntimeKey) (*RuntimeSpec, error)
}
