// 设计依据：docs/数据同步与多后端适配.md §1「Storage Adapter / Router」、§3「六类数据的后端选择」
//                docs/功能开发计划.md 第三批「新增数据类别，流经已有路由」

package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/memory"
	memoryinmemory "trpc.group/trpc-go/trpc-agent-go/memory/inmemory"
	memorymysql "trpc.group/trpc-go/trpc-agent-go/memory/mysql"
	memoryredis "trpc.group/trpc-go/trpc-agent-go/memory/redis"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// MemoryRouter routes long-term memory to a per-tenant backend.
//
// It exists because the platform's multi-backend routing was, until now, true
// of exactly one data type. Session traffic flowed through the router while
// the Memory, Summary, Knowledge and Artifact `DataType` constants had zero
// use sites — the routing was real but only one kind of data ever reached it.
//
// The isolation mechanism is the same one sessions use and deliberately so:
// `memory.UserKey.AppName` carries `tenant/agent`, exactly as the session
// service's app name does. Two tenants that pick the same user id therefore
// address different memories without either backend knowing about tenancy.
// Reusing the mechanism rather than inventing a second one means there is one
// place where cross-tenant leakage could occur, not two.
//
// Why memory needs routing at all, when one shared store would be simpler:
// memory is the longest-lived data the platform holds. A tenant that must keep
// it in its own database — for residency, retention or deletion guarantees —
// cannot be served by a shared instance, and that requirement does not arrive
// at the same time for every tenant.
type MemoryRouter struct {
	backends map[string]memory.Service
	kinds    map[string]string

	defaults map[types.DataType]string
	rules    []config.StorageRule

	metrics StorageMetrics

	log      *slog.Logger
	closeMu  sync.Mutex
	isClosed bool
}

var _ memory.Service = (*MemoryRouter)(nil)

// NewMemoryRouter builds a memory router from the same configuration the
// session router reads.
//
// Sharing the configuration shape is the point: an operator who has already
// expressed "tenant-acme goes to MySQL" should not have to say it twice in a
// different syntax for a different data type. The rules carry a `data_type`
// field precisely so one rule set can describe both.
//
// Returns nil without error when no memory default is configured. Memory is
// optional in a way sessions are not — an agent runs fine without long-term
// recall — so its absence must not stop the Worker from starting.
func NewMemoryRouter(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*MemoryRouter, error) {
	if logger == nil {
		logger = slog.Default()
	}

	def := cfg.Storage.Defaults[string(types.DataTypeMemory)]
	if def == "" {
		logger.Info("no memory backend configured; long-term memory is disabled")
		return nil, nil
	}

	r := &MemoryRouter{
		backends: make(map[string]memory.Service),
		kinds:    make(map[string]string),
		defaults: make(map[types.DataType]string),
		log:      logger,
	}

	// Only backends a memory rule can actually select are built. Building all
	// of them would open connections nothing uses, and a memory service on a
	// backend that only ever serves sessions is a resource nobody accounts for.
	needed := map[string]bool{def: true}
	for _, rule := range cfg.Storage.Rules {
		if rule.DataType == string(types.DataTypeMemory) || rule.DataType == "" {
			needed[rule.Backend] = true
		}
	}

	for _, b := range cfg.Storage.Backends {
		if !needed[b.Name] {
			continue
		}
		svc, err := buildMemoryBackend(b, cfg)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("build memory backend %q: %w", b.Name, err)
		}
		r.backends[b.Name] = svc
		r.kinds[b.Name] = b.Kind
		logger.Info("memory backend ready", "name", b.Name, "kind", b.Kind)
	}

	r.defaults[types.DataTypeMemory] = def
	r.rules = append(r.rules, cfg.Storage.Rules...)

	if _, ok := r.backends[def]; !ok {
		r.Close()
		return nil, fmt.Errorf("memory default backend %q is not registered", def)
	}
	return r, nil
}

// WithMetrics attaches a recorder, so memory latency is visible per backend
// just as session latency is. Routing that cannot be measured cannot be
// justified.
func (r *MemoryRouter) WithMetrics(m StorageMetrics) *MemoryRouter {
	if r != nil {
		r.metrics = m
	}
	return r
}

func buildMemoryBackend(b config.BackendConfig, cfg *config.Config) (memory.Service, error) {
	switch b.Kind {
	case KindInMemory:
		return memoryinmemory.NewMemoryService(), nil

	case KindMySQL:
		// Its own database, for the same reason the session backend needs one:
		// the framework manages its own schema, and pointing it at the
		// control-plane database makes schema verification fail. That is how
		// the session collision was found — the Worker refused to start.
		dsn := b.DSNRef
		if dsn == "" {
			return nil, fmt.Errorf(
				"backend %q needs its own dsn_ref for memory storage", b.Name)
		}
		resolved, err := cfg.ResolveSecret(dsn)
		if err != nil {
			resolved = dsn
		}
		svc, err := memorymysql.NewService(memorymysql.WithMySQLClientDSN(resolved))
		if err != nil {
			return nil, fmt.Errorf("mysql memory service: %w", err)
		}
		return svc, nil

	case KindRedis:
		svc, err := memoryredis.NewService(
			memoryredis.WithRedisClientURL(redisURL(cfg.Redis)))
		if err != nil {
			return nil, fmt.Errorf("redis memory service: %w", err)
		}
		return svc, nil

	default:
		return nil, fmt.Errorf("unsupported memory backend kind %q", b.Kind)
	}
}

// pick resolves the backend for a user key.
//
// The tenant comes from the key's app name rather than from the context. That
// is not a shortcut: the framework calls memory tools from inside its own
// execution, and by then the call may have crossed a goroutine that carries a
// derived context. The app name travels with the data itself, so it cannot be
// lost the way a context value can.
func (r *MemoryRouter) pick(userKey memory.UserKey) (memory.Service, string, error) {
	tenantID, agentAppID, ok := types.ParseAppName(userKey.AppName)
	if !ok {
		// Refusing beats guessing. An unparseable app name means the tenant is
		// unknown, and picking a default would write one tenant's memory into
		// whichever backend happened to be first — the exact failure the
		// router exists to prevent.
		return nil, "", fmt.Errorf(
			"%w: app name %q does not carry a tenant", ErrNoBackend, userKey.AppName)
	}

	ref, err := r.resolve(tenantID, agentAppID)
	if err != nil {
		return nil, "", err
	}
	svc, ok := r.backends[ref.Name]
	if !ok {
		return nil, "", fmt.Errorf("memory backend %q is not registered", ref.Name)
	}
	return svc, ref.Name, nil
}

// resolve applies the same most-specific-rule-wins precedence the session
// router uses, restricted to memory rules.
func (r *MemoryRouter) resolve(tenantID, agentAppID string) (types.BackendRef, error) {
	best, bestScore := "", -1
	for _, rule := range r.rules {
		// A rule with no data type applies to every type; one naming a
		// different type does not apply here.
		if rule.DataType != "" && rule.DataType != string(types.DataTypeMemory) {
			continue
		}
		if rule.TenantID != "" && rule.TenantID != tenantID {
			continue
		}
		if rule.AgentAppID != "" && rule.AgentAppID != agentAppID {
			continue
		}

		score := 0
		if rule.TenantID != "" {
			score += 4
		}
		if rule.AgentAppID != "" {
			score += 2
		}
		if rule.DataType != "" {
			score++
		}
		if score > bestScore {
			best, bestScore = rule.Backend, score
		}
	}

	if best == "" {
		best = r.defaults[types.DataTypeMemory]
	}
	if best == "" {
		return types.BackendRef{}, fmt.Errorf("%w: tenant=%s agent=%s type=memory",
			ErrNoBackend, tenantID, agentAppID)
	}
	return types.BackendRef{Name: best, Kind: r.kinds[best]}, nil
}

// observeMemory times one delegated call, labelled with the backend it went to.
func (r *MemoryRouter) observeMemory(ctx context.Context, backend, op string, fn func() error) error {
	if r.metrics == nil {
		return fn()
	}
	start := time.Now()
	err := fn()
	r.metrics.StorageCall(ctx, backend, op, start, err)
	return err
}

// ---------------------------------------------------------------------------
// memory.Service
// ---------------------------------------------------------------------------

// AddMemory stores one memory for a user.
func (r *MemoryRouter) AddMemory(
	ctx context.Context, userKey memory.UserKey, mem string,
	topics []string, opts ...memory.AddOption,
) error {
	svc, backend, err := r.pick(userKey)
	if err != nil {
		return err
	}
	return r.observeMemory(ctx, backend, "memory_add", func() error {
		return svc.AddMemory(ctx, userKey, mem, topics, opts...)
	})
}

// UpdateMemory edits an existing memory.
//
// memory.Key carries its own app name, so the tenant is recoverable here too;
// the conversion exists only because the two key types differ.
func (r *MemoryRouter) UpdateMemory(
	ctx context.Context, memoryKey memory.Key, mem string,
	topics []string, opts ...memory.UpdateOption,
) error {
	svc, backend, err := r.pick(memory.UserKey{
		AppName: memoryKey.AppName, UserID: memoryKey.UserID,
	})
	if err != nil {
		return err
	}
	return r.observeMemory(ctx, backend, "memory_update", func() error {
		return svc.UpdateMemory(ctx, memoryKey, mem, topics, opts...)
	})
}

// DeleteMemory removes one memory.
func (r *MemoryRouter) DeleteMemory(ctx context.Context, memoryKey memory.Key) error {
	svc, backend, err := r.pick(memory.UserKey{
		AppName: memoryKey.AppName, UserID: memoryKey.UserID,
	})
	if err != nil {
		return err
	}
	return r.observeMemory(ctx, backend, "memory_delete", func() error {
		return svc.DeleteMemory(ctx, memoryKey)
	})
}

// ClearMemories removes every memory for one user.
func (r *MemoryRouter) ClearMemories(ctx context.Context, userKey memory.UserKey) error {
	svc, backend, err := r.pick(userKey)
	if err != nil {
		return err
	}
	return r.observeMemory(ctx, backend, "memory_clear", func() error {
		return svc.ClearMemories(ctx, userKey)
	})
}

// ReadMemories reads a user's memories.
func (r *MemoryRouter) ReadMemories(
	ctx context.Context, userKey memory.UserKey, limit int,
) ([]*memory.Entry, error) {
	svc, backend, err := r.pick(userKey)
	if err != nil {
		return nil, err
	}
	var out []*memory.Entry
	err = r.observeMemory(ctx, backend, "memory_read", func() error {
		var readErr error
		out, readErr = svc.ReadMemories(ctx, userKey, limit)
		return readErr
	})
	return out, err
}

// SearchMemories searches a user's memories.
func (r *MemoryRouter) SearchMemories(
	ctx context.Context, userKey memory.UserKey, query string,
	opts ...memory.SearchOption,
) ([]*memory.Entry, error) {
	svc, backend, err := r.pick(userKey)
	if err != nil {
		return nil, err
	}
	var out []*memory.Entry
	err = r.observeMemory(ctx, backend, "memory_search", func() error {
		var searchErr error
		out, searchErr = svc.SearchMemories(ctx, userKey, query, opts...)
		return searchErr
	})
	return out, err
}

// Tools returns the memory tools the model can call.
//
// Taken from the default backend rather than merged across all of them. The
// tool *declarations* are identical whichever backend serves a tenant — they
// describe the operation, not the storage — and returning several copies of
// the same tool name would give the model duplicate options.
//
// Each call still routes per tenant when it executes, because the tools reach
// this router, not the backend they were declared by.
func (r *MemoryRouter) Tools() []tool.Tool {
	if r == nil {
		return nil
	}
	def := r.defaults[types.DataTypeMemory]
	if svc, ok := r.backends[def]; ok {
		return svc.Tools()
	}
	return nil
}

// EnqueueAutoMemoryJob hands a transcript to the backend for extraction.
//
// The session's app name is the tenant carrier here, the same as everywhere
// else in this file.
func (r *MemoryRouter) EnqueueAutoMemoryJob(ctx context.Context, sess *session.Session) error {
	if sess == nil {
		return errors.New("memory router: nil session")
	}
	svc, backend, err := r.pick(memory.UserKey{
		AppName: sess.AppName, UserID: sess.UserID,
	})
	if err != nil {
		return err
	}
	return r.observeMemory(ctx, backend, "memory_enqueue", func() error {
		return svc.EnqueueAutoMemoryJob(ctx, sess)
	})
}

// Close releases every backend.
//
// Errors are collected rather than returned on the first failure: a backend
// that fails to close must not stop the others from being released, or a
// shutdown leaks connections.
func (r *MemoryRouter) Close() error {
	if r == nil {
		return nil
	}
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.isClosed {
		return nil
	}
	r.isClosed = true

	var firstErr error
	for name, svc := range r.backends {
		if err := svc.Close(); err != nil {
			r.log.Warn("closing memory backend failed", "backend", name, "error", err.Error())
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// BackendFor reports which backend serves a tenant, for diagnostics and for
// the acceptance script to assert that two tenants really do land in
// different stores.
func (r *MemoryRouter) BackendFor(tenantID, agentAppID string) (types.BackendRef, error) {
	if r == nil {
		return types.BackendRef{}, ErrNoBackend
	}
	return r.resolve(tenantID, agentAppID)
}
