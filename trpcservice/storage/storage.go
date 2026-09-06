// 设计依据：docs/数据同步与多后端适配.md §1「Storage Adapter / Router」
//                docs/框架复用与扩展.md §3.3「Storage Router」

// Package storage routes data access to a per-tenant backend.
//
// The router implements the framework's session.Service and is injected with
// runner.WithSessionService. That places tenant routing on the framework's own
// execution path: the Runner calls session.Service as usual, the router picks
// the backend for the tenant carried by the call, and delegates. No second
// data-access layer sits beside the framework.
//
// Phase one registers an in-memory and a Redis backend and routes between
// them from configuration. Phase two reads the same mapping from
// backend_configs and backend_bindings; only the provider changes, not this
// interface.
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/session"
	"trpc.group/trpc-go/trpc-agent-go/session/inmemory"
	sessionmysql "trpc.group/trpc-go/trpc-agent-go/session/mysql"
	sessionredis "trpc.group/trpc-go/trpc-agent-go/session/redis"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Backend kinds understood by this package.
const (
	KindInMemory = "inmemory"
	KindRedis    = "redis"
	KindMySQL    = "mysql"
)

// ErrNoBackend means no backend serves the requested triple.
var ErrNoBackend = errors.New("storage: no backend for tenant and data type")

// Router dispatches session access to a backend chosen by tenant, agent and
// data type.
type Router struct {
	backends map[string]session.Service
	kinds    map[string]string

	defaults map[types.DataType]string
	rules    []config.StorageRule

	// metrics receives per-backend latency. Nil disables recording rather
	// than requiring a nil check at each of the seventeen delegations.
	metrics StorageMetrics

	log      *slog.Logger
	closeMu  sync.Mutex
	isClosed bool
}

// StorageMetrics receives session backend timings.
//
// An interface so this package does not depend on the metrics package. The
// backend name is part of the measurement because the whole point of routing
// is that different tenants sit on different stores with different latency.
type StorageMetrics interface {
	StorageCall(ctx context.Context, backend, op string, start time.Time, err error)
}

// WithMetrics attaches a recorder.
func (r *Router) WithMetrics(m StorageMetrics) *Router {
	r.metrics = m
	return r
}

// observe times one delegated call.
//
// Wrapping rather than repeating the timing in every method: seventeen
// hand-written copies would drift, and a missing one shows up as a backend
// that silently reports no latency.
func (r *Router) observe(ctx context.Context, op string, fn func() error) error {
	if r.metrics == nil {
		return fn()
	}
	start := time.Now()
	err := fn()
	r.metrics.StorageCall(ctx, r.backendFor(ctx), op, start, err)
	return err
}

// backendFor names the backend a call was routed to, for the metric label.
// Unknown resolves to "unrouted" rather than being dropped: a call whose
// tenant could not be determined is worth seeing.
func (r *Router) backendFor(ctx context.Context) string {
	tenantID, agentAppID := types.TenantID(ctx), ""
	if rc, err := types.FromContext(ctx); err == nil {
		agentAppID = rc.AgentAppID
	}
	if tenantID == "" {
		return "unrouted"
	}
	ref, err := r.Resolve(ctx, tenantID, agentAppID, types.DataTypeSession)
	if err != nil {
		return "unrouted"
	}
	return ref.Name
}

var _ types.StorageRouter = (*Router)(nil)

// New builds a router from configuration.
//
// Backends are created eagerly so a misconfigured connection fails at startup
// rather than on the first user message.
func New(ctx context.Context, cfg *config.Config, logger *slog.Logger) (*Router, error) {
	if logger == nil {
		logger = slog.Default()
	}

	r := &Router{
		backends: make(map[string]session.Service),
		kinds:    make(map[string]string),
		defaults: make(map[types.DataType]string),
		log:      logger,
	}

	for _, b := range cfg.Storage.Backends {
		svc, err := buildBackend(b, cfg)
		if err != nil {
			r.Close()
			return nil, fmt.Errorf("build backend %q: %w", b.Name, err)
		}
		r.backends[b.Name] = svc
		r.kinds[b.Name] = b.Kind
		logger.Info("storage backend ready", "name", b.Name, "kind", b.Kind)
	}

	for dtName, backend := range cfg.Storage.Defaults {
		r.defaults[types.DataType(dtName)] = backend
	}
	r.rules = append(r.rules, cfg.Storage.Rules...)

	// Sessions are the one data type the Runner cannot work without, so a
	// missing default is a startup error rather than a surprise later.
	if _, ok := r.defaults[types.DataTypeSession]; !ok {
		r.Close()
		return nil, fmt.Errorf("storage.defaults.session is required")
	}
	return r, nil
}

func buildBackend(b config.BackendConfig, cfg *config.Config) (session.Service, error) {
	switch b.Kind {
	case KindInMemory:
		return inmemory.NewSessionService(), nil

	case KindMySQL:
		// A SQL-backed session service: durable across restarts and
		// queryable, at the cost of higher write latency than Redis. Which
		// tenant gets which is the trade-off the router exists to express.
		//
		// It needs its **own** database, not the control-plane one.
		//
		// The framework manages its own session schema and creates a
		// session_events table with an app_name column. The platform's
		// data model already defines a session_events table of its own,
		// keyed by (session_id, sequence) for ordering. Same name, different
		// shape: pointing both at one database makes the framework's schema
		// verification fail outright — which is how this was found, the
		// Worker refused to start.
		//
		// Separate databases is the right answer rather than a workaround:
		// the platform's table is the durable conversation history it owns,
		// the framework's is that backend's internal storage. Conflating
		// them would also mean a backend change could rewrite history.
		dsn := b.DSNRef
		if dsn == "" {
			return nil, fmt.Errorf(
				"backend %q needs its own dsn_ref: the framework's session schema "+
					"collides with the platform's session_events table", b.Name)
		}
		resolved, err := cfg.ResolveSecret(dsn)
		if err != nil {
			// Not a secret reference: treat it as a literal DSN, which is
			// what a local development config carries.
			resolved = dsn
		}
		svc, err := sessionmysql.NewService(
			sessionmysql.WithMySQLClientDSN(resolved))
		if err != nil {
			return nil, fmt.Errorf("mysql session service: %w", err)
		}
		return svc, nil

	case KindRedis:
		// The session backend reuses the same Redis the scheduler uses. They
		// occupy disjoint key spaces, so one instance is enough in phase one
		// and the router still demonstrates real per-tenant selection.
		url := redisURL(cfg.Redis)
		svc, err := sessionredis.NewService(sessionredis.WithRedisClientURL(url))
		if err != nil {
			return nil, fmt.Errorf("redis session service: %w", err)
		}
		return svc, nil

	default:
		return nil, fmt.Errorf("unsupported backend kind %q", b.Kind)
	}
}

// redisURL builds a redis:// URL without embedding the password in anything
// that gets logged. The caller passes it straight to the client builder.
func redisURL(rc config.RedisConfig) string {
	if rc.Password != "" {
		return fmt.Sprintf("redis://:%s@%s/%d", rc.Password, rc.Addr, rc.DB)
	}
	return fmt.Sprintf("redis://%s/%d", rc.Addr, rc.DB)
}

// Resolve reports which backend serves this triple.
//
// The most specific rule wins: an exact tenant plus agent plus data type beats
// a tenant-wide rule, which beats the default. Matching is explicit rather
// than by map lookup so the precedence is visible.
func (r *Router) Resolve(_ context.Context, tenantID, agentAppID string, dt types.DataType) (types.BackendRef, error) {
	best, bestScore := "", -1
	for _, rule := range r.rules {
		if rule.DataType != "" && types.DataType(rule.DataType) != dt {
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
		best = r.defaults[dt]
	}
	if best == "" {
		return types.BackendRef{}, fmt.Errorf("%w: tenant=%s agent=%s type=%s",
			ErrNoBackend, tenantID, agentAppID, dt)
	}
	if _, ok := r.backends[best]; !ok {
		return types.BackendRef{}, fmt.Errorf("backend %q is referenced but not registered", best)
	}
	return types.BackendRef{Name: best, Kind: r.kinds[best]}, nil
}

// SessionService returns the backing service for a tenant and agent.
func (r *Router) SessionService(ctx context.Context, tenantID, agentAppID string) (session.Service, error) {
	ref, err := r.Resolve(ctx, tenantID, agentAppID, types.DataTypeSession)
	if err != nil {
		return nil, err
	}
	return r.backends[ref.Name], nil
}

// Backends lists registered backend names and kinds, for diagnostics.
func (r *Router) Backends() map[string]string {
	out := make(map[string]string, len(r.kinds))
	for name, kind := range r.kinds {
		out[name] = kind
	}
	return out
}

// ---------------------------------------------------------------------------
// session.Service delegation
//
// Each method resolves the backend from the tenant encoded in the app name and
// forwards unchanged. Resolution failures are returned, never defaulted: a
// call whose tenant cannot be determined must not land in an arbitrary
// backend.
// ---------------------------------------------------------------------------

// forKey resolves the backend for a session key.
func (r *Router) forKey(ctx context.Context, key session.Key) (session.Service, error) {
	tenantID, agentAppID, ok := types.ParseAppName(key.AppName)
	if !ok {
		return nil, fmt.Errorf("storage: app name %q is not tenant/agent", key.AppName)
	}
	return r.SessionService(ctx, tenantID, agentAppID)
}

// forUserKey resolves the backend for a user key.
func (r *Router) forUserKey(ctx context.Context, key session.UserKey) (session.Service, error) {
	tenantID, agentAppID, ok := types.ParseAppName(key.AppName)
	if !ok {
		return nil, fmt.Errorf("storage: app name %q is not tenant/agent", key.AppName)
	}
	return r.SessionService(ctx, tenantID, agentAppID)
}

// forAppName resolves the backend for a bare app name.
func (r *Router) forAppName(ctx context.Context, appName string) (session.Service, error) {
	tenantID, agentAppID, ok := types.ParseAppName(appName)
	if !ok {
		return nil, fmt.Errorf("storage: app name %q is not tenant/agent", appName)
	}
	return r.SessionService(ctx, tenantID, agentAppID)
}

func (r *Router) CreateSession(ctx context.Context, key session.Key, state session.StateMap, opts ...session.Option) (*session.Session, error) {
	svc, err := r.forKey(ctx, key)
	if err != nil {
		return nil, err
	}
	var out *session.Session
	err = r.observe(ctx, "create_session", func() error {
		var e error
		out, e = svc.CreateSession(ctx, key, state, opts...)
		return e
	})
	return out, err
}

func (r *Router) GetSession(ctx context.Context, key session.Key, opts ...session.Option) (*session.Session, error) {
	svc, err := r.forKey(ctx, key)
	if err != nil {
		return nil, err
	}
	var out *session.Session
	err = r.observe(ctx, "get_session", func() error {
		var e error
		out, e = svc.GetSession(ctx, key, opts...)
		return e
	})
	return out, err
}

func (r *Router) ListSessions(ctx context.Context, key session.UserKey, opts ...session.Option) ([]*session.Session, error) {
	svc, err := r.forUserKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return svc.ListSessions(ctx, key, opts...)
}

func (r *Router) DeleteSession(ctx context.Context, key session.Key, opts ...session.Option) error {
	svc, err := r.forKey(ctx, key)
	if err != nil {
		return err
	}
	return svc.DeleteSession(ctx, key, opts...)
}

func (r *Router) UpdateAppState(ctx context.Context, appName string, state session.StateMap) error {
	svc, err := r.forAppName(ctx, appName)
	if err != nil {
		return err
	}
	return svc.UpdateAppState(ctx, appName, state)
}

func (r *Router) DeleteAppState(ctx context.Context, appName string, key string) error {
	svc, err := r.forAppName(ctx, appName)
	if err != nil {
		return err
	}
	return svc.DeleteAppState(ctx, appName, key)
}

func (r *Router) ListAppStates(ctx context.Context, appName string) (session.StateMap, error) {
	svc, err := r.forAppName(ctx, appName)
	if err != nil {
		return nil, err
	}
	return svc.ListAppStates(ctx, appName)
}

func (r *Router) UpdateUserState(ctx context.Context, key session.UserKey, state session.StateMap) error {
	svc, err := r.forUserKey(ctx, key)
	if err != nil {
		return err
	}
	return svc.UpdateUserState(ctx, key, state)
}

func (r *Router) ListUserStates(ctx context.Context, key session.UserKey) (session.StateMap, error) {
	svc, err := r.forUserKey(ctx, key)
	if err != nil {
		return nil, err
	}
	return svc.ListUserStates(ctx, key)
}

func (r *Router) DeleteUserState(ctx context.Context, key session.UserKey, stateKey string) error {
	svc, err := r.forUserKey(ctx, key)
	if err != nil {
		return err
	}
	return svc.DeleteUserState(ctx, key, stateKey)
}

func (r *Router) UpdateSessionState(ctx context.Context, key session.Key, state session.StateMap) error {
	svc, err := r.forKey(ctx, key)
	if err != nil {
		return err
	}
	return r.observe(ctx, "update_state", func() error {
		return svc.UpdateSessionState(ctx, key, state)
	})
}

func (r *Router) AppendEvent(ctx context.Context, sess *session.Session, e *event.Event, opts ...session.Option) error {
	if sess == nil {
		return errors.New("storage: nil session")
	}
	svc, err := r.forAppName(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return r.observe(ctx, "append_event", func() error {
		return svc.AppendEvent(ctx, sess, e, opts...)
	})
}

func (r *Router) CreateSessionSummary(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if sess == nil {
		return errors.New("storage: nil session")
	}
	svc, err := r.forAppName(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return svc.CreateSessionSummary(ctx, sess, filterKey, force)
}

func (r *Router) EnqueueSummaryJob(ctx context.Context, sess *session.Session, filterKey string, force bool) error {
	if sess == nil {
		return errors.New("storage: nil session")
	}
	svc, err := r.forAppName(ctx, sess.AppName)
	if err != nil {
		return err
	}
	return svc.EnqueueSummaryJob(ctx, sess, filterKey, force)
}

func (r *Router) GetSessionSummaryText(ctx context.Context, sess *session.Session, opts ...session.SummaryOption) (string, bool) {
	if sess == nil {
		return "", false
	}
	svc, err := r.forAppName(ctx, sess.AppName)
	if err != nil {
		// Summaries are an optimisation; a routing failure degrades to "no
		// summary" rather than failing the reply.
		r.log.Warn("summary lookup could not resolve backend",
			"app_name", sess.AppName, "error", err.Error())
		return "", false
	}
	return svc.GetSessionSummaryText(ctx, sess, opts...)
}

// Close releases every backend. It is safe to call more than once.
func (r *Router) Close() error {
	r.closeMu.Lock()
	defer r.closeMu.Unlock()
	if r.isClosed {
		return nil
	}
	r.isClosed = true

	var errs []error
	for name, svc := range r.backends {
		if err := svc.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close backend %q: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
