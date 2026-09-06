// 设计依据：docs/多租户与节点部署设计.md §6「Runtime 装配与缓存」
//                docs/dev/一期实现内容.md §4「结构性约束」

// Package agent assembles Runtimes from control-plane configuration.
//
// The package holds no agent definitions. A Runtime is built at request time
// from a tenant's published version — its prompt, model, tools, extensions,
// MCP servers and skills all come from the database. That distinction is what
// makes the platform multi-tenant: an agent hardcoded here would be a
// single-tenant service wearing a multi-tenant schema.
//
// Three constraints shape the code:
//
//   - Assembly returns agent.Agent, the framework interface, never a concrete
//     *llmagent.LLMAgent. Adding graph orchestration later becomes one more
//     branch inside the assembler rather than a change at every call site.
//   - Capabilities are mounted by walking a list, never by naming known
//     extensions. A second guardrail is then a row, not a code change.
//   - The cache key always includes the tenant. Two tenants may name an agent
//     identically and both call a version "v1"; a key without the tenant would
//     let them share models, tools and credentials.
package agent

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/agent"
	"trpc.group/trpc-go/trpc-agent-go/agent/llmagent"
	"trpc.group/trpc-go/trpc-agent-go/memory"
	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/model/openai"
	"trpc.group/trpc-go/trpc-agent-go/runner"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Provider assembles and caches Runtimes.
type Provider struct {
	cfg        *config.Config
	specs      types.SpecLoader
	router     types.StorageRouter
	memory     MemoryService
	tools      *platformtool.Registry
	extensions *ExtensionRegistry
	policies   PolicyDeps
	log        *slog.Logger

	mu    sync.Mutex
	cache map[types.RuntimeKey]*list.Element
	// lru orders entries by last use, most recent at the front.
	lru *list.List
	// building tracks in-flight assemblies so concurrent requests for the
	// same key wait rather than each building their own model client and
	// tool set.
	building map[types.RuntimeKey]chan struct{}
}

var _ types.RuntimeProvider = (*Provider)(nil)

// entry is one cached Runtime plus its bookkeeping.
type entry struct {
	key      types.RuntimeKey
	runtime  *types.Runtime
	lastUsed time.Time
}

// Deps are the collaborators a Provider needs.
type Deps struct {
	Config     *config.Config
	Specs      types.SpecLoader
	Router     types.StorageRouter
	Tools      *platformtool.Registry
	Extensions *ExtensionRegistry
	// Memory routes long-term recall to a per-tenant backend. Nil disables
	// memory rather than failing assembly: an agent runs fine without
	// long-term recall, and a deployment that has not configured a memory
	// backend must still start.
	Memory MemoryService
	// Policies are the platform services governance extensions depend on.
	// Left zero, policies needing them refuse to mount rather than silently
	// becoming no-ops that look like protection.
	Policies PolicyDeps
	Logger   *slog.Logger
}

// NewProvider builds a Provider.
func NewProvider(d Deps) (*Provider, error) {
	switch {
	case d.Config == nil:
		return nil, errors.New("agent: config is required")
	case d.Specs == nil:
		return nil, errors.New("agent: spec loader is required")
	case d.Router == nil:
		return nil, errors.New("agent: storage router is required")
	}

	tools := d.Tools
	if tools == nil {
		tools = platformtool.NewRegistry()
	}
	exts := d.Extensions
	if exts == nil {
		exts = NewExtensionRegistry()
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}

	return &Provider{
		cfg: d.Config, specs: d.Specs, router: d.Router, memory: d.Memory,
		tools: tools, extensions: exts, policies: d.Policies, log: logger,
		cache:    make(map[types.RuntimeKey]*list.Element),
		lru:      list.New(),
		building: make(map[types.RuntimeKey]chan struct{}),
	}, nil
}

// MemoryService is the subset of the framework's memory.Service the assembler
// needs.
//
// Declared as an interface here, rather than importing the framework's type
// into this package's Deps, so that a test can inject a fake and so the agent
// package does not depend on the storage package. It is deliberately the full
// framework interface: narrowing it would mean the router could not be passed
// straight to runner.WithMemoryService.
type MemoryService = memory.Service

// Get returns the Runtime for key, assembling it on first use.
//
// Concurrent callers for the same key assemble once: the first takes the build
// slot, the rest wait on a channel and then read the cache. Without this, a
// burst of messages for a cold session would each construct a model client and
// a full tool set, and all but one would be discarded.
func (p *Provider) Get(ctx context.Context, key types.RuntimeKey) (*types.Runtime, error) {
	if !key.Valid() {
		return nil, fmt.Errorf("agent: incomplete runtime key %s", key)
	}

	for {
		p.mu.Lock()
		if el, ok := p.cache[key]; ok {
			e := el.Value.(*entry)
			e.lastUsed = time.Now()
			p.lru.MoveToFront(el)
			p.mu.Unlock()
			return e.runtime, nil
		}
		if wait, building := p.building[key]; building {
			p.mu.Unlock()
			select {
			case <-wait:
				continue // builder finished; re-check the cache
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		done := make(chan struct{})
		p.building[key] = done
		p.mu.Unlock()

		rt, err := p.assemble(ctx, key)

		p.mu.Lock()
		delete(p.building, key)
		close(done)
		if err != nil {
			p.mu.Unlock()
			return nil, err
		}
		p.insertLocked(key, rt)
		p.mu.Unlock()
		return rt, nil
	}
}

// insertLocked stores a Runtime and evicts if the cache is over capacity.
// Callers must hold p.mu.
func (p *Provider) insertLocked(key types.RuntimeKey, rt *types.Runtime) {
	e := &entry{key: key, runtime: rt, lastUsed: time.Now()}
	p.cache[key] = p.lru.PushFront(e)

	// Published versions are immutable, so a cached Runtime never goes stale
	// and there is nothing to invalidate. What remains is capacity: each
	// Runtime holds a model client and tool instances, and tenants and
	// versions only accumulate, so an unbounded cache is an eventual OOM.
	for p.cfg.Runtime.CacheSize > 0 && p.lru.Len() > p.cfg.Runtime.CacheSize {
		oldest := p.lru.Back()
		if oldest == nil {
			break
		}
		victim := oldest.Value.(*entry)
		p.lru.Remove(oldest)
		delete(p.cache, victim.key)
		p.log.Info("runtime evicted", "key", victim.key.String(), "reason", "capacity")
		go closeRuntime(victim.runtime, p.log)
	}
}

// EvictIdle releases Runtimes unused for longer than the configured TTL.
// Callers run this periodically; it is separate from Get so an idle tenant
// does not hold a model client indefinitely.
func (p *Provider) EvictIdle() int {
	if p.cfg.Runtime.IdleTTL <= 0 {
		return 0
	}
	cutoff := time.Now().Add(-p.cfg.Runtime.IdleTTL)

	p.mu.Lock()
	var stale []*entry
	for el := p.lru.Back(); el != nil; {
		prev := el.Prev()
		e := el.Value.(*entry)
		if e.lastUsed.Before(cutoff) {
			p.lru.Remove(el)
			delete(p.cache, e.key)
			stale = append(stale, e)
		}
		el = prev
	}
	p.mu.Unlock()

	for _, e := range stale {
		p.log.Info("runtime evicted", "key", e.key.String(), "reason", "idle")
		closeRuntime(e.runtime, p.log)
	}
	return len(stale)
}

// Invalidate drops a cached Runtime, for version retirement.
func (p *Provider) Invalidate(_ context.Context, key types.RuntimeKey) error {
	p.mu.Lock()
	el, ok := p.cache[key]
	if ok {
		p.lru.Remove(el)
		delete(p.cache, key)
	}
	p.mu.Unlock()

	if ok {
		closeRuntime(el.Value.(*entry).runtime, p.log)
	}
	return nil
}

// Len reports how many Runtimes are cached, for metrics.
func (p *Provider) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lru.Len()
}

// Close releases every cached Runtime.
func (p *Provider) Close() error {
	p.mu.Lock()
	entries := make([]*entry, 0, p.lru.Len())
	for el := p.lru.Front(); el != nil; el = el.Next() {
		entries = append(entries, el.Value.(*entry))
	}
	p.cache = make(map[types.RuntimeKey]*list.Element)
	p.lru = list.New()
	p.mu.Unlock()

	var errs []error
	for _, e := range entries {
		if err := e.runtime.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close runtime %s: %w", e.key, err))
		}
	}
	return errors.Join(errs...)
}

func closeRuntime(rt *types.Runtime, log *slog.Logger) {
	if err := rt.Close(); err != nil {
		log.Warn("closing runtime failed", "key", rt.Key.String(), "error", err.Error())
	}
}

// assemble builds one Runtime from its stored specification.
func (p *Provider) assemble(ctx context.Context, key types.RuntimeKey) (*types.Runtime, error) {
	start := time.Now()

	spec, err := p.specs.Load(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("load spec for %s: %w", key, err)
	}

	mdl, modelNote := p.buildModel(spec)

	tools, err := p.tools.Resolve(spec.Tools)
	if err != nil {
		return nil, fmt.Errorf("resolve tools for %s: %w", key, err)
	}

	agentCallbacks := agent.NewCallbacks()
	modelCallbacks := model.NewCallbacks()
	toolCallbacks := tool.NewCallbacks()
	if err := p.extensions.Mount(spec.Extensions, &MountPoints{
		Agent:  agentCallbacks,
		Model:  modelCallbacks,
		Tool:   toolCallbacks,
		Logger: p.log,
		Spec:   spec,
		Deps:   p.policies,
	}); err != nil {
		return nil, fmt.Errorf("mount extensions for %s: %w", key, err)
	}

	// MCP servers and skills are empty in phase one. The lists are still read
	// and reported, so wiring them later is a data change rather than a new
	// branch in this function.
	if len(spec.MCPServers) > 0 || len(spec.Skills) > 0 {
		p.log.Info("mcp and skill bindings present but not yet mounted",
			"key", key.String(), "mcp", len(spec.MCPServers), "skills", len(spec.Skills))
	}

	opts := []llmagent.Option{
		llmagent.WithModel(mdl),
		llmagent.WithDescription(fmt.Sprintf("tenant %s agent %s version %s",
			key.TenantID, key.AgentAppID, key.AgentVersion)),
		llmagent.WithAgentCallbacks(agentCallbacks),
		llmagent.WithModelCallbacks(modelCallbacks),
		llmagent.WithToolCallbacks(toolCallbacks),
	}
	if spec.SystemPrompt != "" {
		opts = append(opts, llmagent.WithInstruction(spec.SystemPrompt))
	}
	if len(tools) > 0 {
		opts = append(opts, llmagent.WithTools(tools))
	}
	if gc, ok := generationConfig(spec.ModelParams); ok {
		opts = append(opts, llmagent.WithGenerationConfig(gc))
	}

	// Declared as the interface, not the concrete type, so a future branch
	// returning a graph agent needs no change beyond this function.
	var built agent.Agent = llmagent.New(agentName(key), opts...)

	// The storage router is the session service. Injecting it here is what
	// puts per-tenant backend selection on the framework's own execution path
	// instead of beside it.
	//
	// The memory router goes in the same way and for the same reason. Until it
	// did, the platform's multi-backend routing was true of exactly one data
	// type: sessions flowed through the router while the Memory, Summary,
	// Knowledge and Artifact DataType constants had zero use sites.
	runOpts := []runner.Option{runner.WithSessionService(p.router)}
	if p.memory != nil {
		runOpts = append(runOpts, runner.WithMemoryService(p.memory))
	}
	run := runner.NewRunner(
		types.AppName(key.TenantID, key.AgentAppID),
		built,
		runOpts...,
	)

	p.log.Info("runtime assembled",
		"key", key.String(),
		"model", spec.ModelName,
		"tools", len(tools),
		"extensions", len(spec.Extensions),
		"build_ms", time.Since(start).Milliseconds(),
		"model_note", modelNote)

	return &types.Runtime{Key: key, Agent: built, Runner: run, Spec: spec}, nil
}

// buildModel resolves the version's model, falling back to a visible stub when
// the credential cannot be resolved.
//
// Falling back rather than failing is a deliberate phase-one choice: it keeps
// the whole pipeline verifiable without a provider account. The stub announces
// itself in its output, so a stand-in can never be mistaken for a real answer.
func (p *Provider) buildModel(spec *types.RuntimeSpec) (model.Model, string) {
	apiKey, err := p.cfg.ResolveSecret(spec.ModelAPIKeyRef)
	if err != nil || apiKey == "" {
		reason := describeMissingKey(spec.ModelAPIKeyRef, err)
		p.log.Warn("falling back to stub model",
			"key", spec.Key.String(), "model", spec.ModelName, "reason", reason)
		return newEchoModel(spec.ModelName, reason), "stub"
	}

	opts := []openai.Option{openai.WithAPIKey(apiKey)}
	if baseURL, ok := p.cfg.ModelBaseURL(spec.ModelName); ok {
		opts = append(opts, openai.WithBaseURL(baseURL))
	}
	return openai.New(spec.ModelName, opts...), "live"
}

// generationConfig maps the version's stored model parameters onto the
// framework's configuration. Unknown keys are ignored rather than rejected, so
// a version can carry parameters this build does not yet understand.
func generationConfig(params map[string]any) (model.GenerationConfig, bool) {
	if len(params) == 0 {
		return model.GenerationConfig{}, false
	}

	var gc model.GenerationConfig
	set := false
	if v, ok := numeric(params["temperature"]); ok {
		gc.Temperature = &v
		set = true
	}
	if v, ok := numeric(params["top_p"]); ok {
		gc.TopP = &v
		set = true
	}
	if v, ok := numeric(params["max_tokens"]); ok {
		n := int(v)
		gc.MaxTokens = &n
		set = true
	}
	return gc, set
}

func numeric(v any) (float64, bool) {
	switch t := v.(type) {
	case float64:
		return t, true
	case float32:
		return float64(t), true
	case int:
		return float64(t), true
	case int64:
		return float64(t), true
	default:
		return 0, false
	}
}

// agentName is the identity the framework uses in events and traces. It
// carries the tenant so a trace from a shared Worker is attributable.
func agentName(key types.RuntimeKey) string {
	return fmt.Sprintf("%s-%s-%s", key.TenantID, key.AgentAppID, key.AgentVersion)
}
