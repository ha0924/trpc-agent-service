package storage

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/memory"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests cover memory routing without any backend, because the property
// that matters is *which* backend a call goes to — and getting that wrong is
// how one tenant reads another's memories.
//
// Memory is the longest-lived data the platform holds, so a routing mistake
// here does not surface as a transient error; it surfaces as data in the wrong
// place, discovered much later.

func testRouter(t *testing.T, defaultBackend string, rules ...config.StorageRule) *MemoryRouter {
	t.Helper()
	cfg := &config.Config{}
	cfg.Storage.Backends = []config.BackendConfig{
		{Name: "redis-main", Kind: KindInMemory},
		{Name: "mysql-main", Kind: KindInMemory},
	}
	cfg.Storage.Defaults = map[string]string{
		string(types.DataTypeMemory): defaultBackend,
	}
	cfg.Storage.Rules = rules

	r, err := NewMemoryRouter(context.Background(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewMemoryRouter: %v", err)
	}
	if r == nil {
		t.Fatal("router should not be nil when a default is configured")
	}
	t.Cleanup(func() { r.Close() })
	return r
}

func TestMemoryDisabledWithoutADefault(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.Defaults = map[string]string{}

	// Memory is optional in a way sessions are not: an agent runs fine
	// without recall. Returning nil rather than an error is what lets a
	// deployment that never configured memory still start.
	r, err := NewMemoryRouter(context.Background(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("NewMemoryRouter: %v", err)
	}
	if r != nil {
		t.Fatal("no memory default should disable memory, not build a router")
	}
	// Every method must tolerate the nil receiver, since callers hold it as
	// an interface and cannot easily check.
	if r.Tools() != nil {
		t.Error("nil router should report no tools")
	}
	if err := r.Close(); err != nil {
		t.Errorf("closing a nil router: %v", err)
	}
}

func TestMemoryRoutesByTenantAndDataType(t *testing.T) {
	r := testRouter(t, "redis-main",
		config.StorageRule{
			TenantID: "tenant-acme",
			DataType: string(types.DataTypeMemory),
			Backend:  "mysql-main",
		},
		// A session rule for the same tenant must not affect memory. Without
		// the data-type filter this rule would also capture memory traffic.
		config.StorageRule{
			TenantID: "tenant-demo",
			DataType: string(types.DataTypeSession),
			Backend:  "mysql-main",
		},
	)

	cases := []struct {
		tenant string
		want   string
		why    string
	}{
		{"tenant-acme", "mysql-main", "a memory rule for this tenant"},
		{"tenant-demo", "redis-main", "its only rule names sessions, not memory"},
		{"tenant-other", "redis-main", "no rule, so the default"},
	}
	for _, c := range cases {
		t.Run(c.tenant, func(t *testing.T) {
			ref, err := r.BackendFor(c.tenant, "assistant")
			if err != nil {
				t.Fatalf("BackendFor: %v", err)
			}
			if ref.Name != c.want {
				t.Errorf("backend = %q, want %q (%s)", ref.Name, c.want, c.why)
			}
		})
	}
}

func TestMemoryRuleSpecificityWins(t *testing.T) {
	r := testRouter(t, "redis-main",
		// Tenant-wide.
		config.StorageRule{
			TenantID: "tenant-acme",
			DataType: string(types.DataTypeMemory),
			Backend:  "redis-main",
		},
		// Tenant plus agent — more specific, so it must win regardless of
		// declaration order.
		config.StorageRule{
			TenantID:   "tenant-acme",
			AgentAppID: "special",
			DataType:   string(types.DataTypeMemory),
			Backend:    "mysql-main",
		},
	)

	ref, err := r.BackendFor("tenant-acme", "special")
	if err != nil {
		t.Fatalf("BackendFor: %v", err)
	}
	if ref.Name != "mysql-main" {
		t.Errorf("backend = %q, want mysql-main: tenant+agent beats tenant-wide", ref.Name)
	}

	ref, err = r.BackendFor("tenant-acme", "ordinary")
	if err != nil {
		t.Fatalf("BackendFor: %v", err)
	}
	if ref.Name != "redis-main" {
		t.Errorf("backend = %q, want redis-main for a non-matching agent", ref.Name)
	}
}

func TestUnparseableAppNameIsRefused(t *testing.T) {
	r := testRouter(t, "redis-main")
	ctx := context.Background()

	// An app name that carries no tenant means the tenant is unknown.
	// Falling back to the default would write one tenant's memory into
	// whichever backend happened to be configured first — exactly the
	// failure routing exists to prevent. Refusing is the only safe answer.
	bad := memory.UserKey{AppName: "no-slash-here", UserID: "u1"}

	if err := r.AddMemory(ctx, bad, "secret", nil); !errors.Is(err, ErrNoBackend) {
		t.Errorf("AddMemory with a tenantless app name = %v, want ErrNoBackend", err)
	}
	if _, err := r.ReadMemories(ctx, bad, 10); !errors.Is(err, ErrNoBackend) {
		t.Errorf("ReadMemories = %v, want ErrNoBackend", err)
	}
	if _, err := r.SearchMemories(ctx, bad, "q"); !errors.Is(err, ErrNoBackend) {
		t.Errorf("SearchMemories = %v, want ErrNoBackend", err)
	}
	if err := r.ClearMemories(ctx, bad); !errors.Is(err, ErrNoBackend) {
		t.Errorf("ClearMemories = %v, want ErrNoBackend", err)
	}
}

func TestSameUserIDInTwoTenantsIsIsolated(t *testing.T) {
	r := testRouter(t, "redis-main")
	ctx := context.Background()

	// Two tenants both using user id "u1" — a realistic collision, since
	// tenants pick their own user ids. The app name is what keeps them apart,
	// so a write under one must not be readable under the other.
	keyA := memory.UserKey{AppName: types.AppName("tenant-a", "assistant"), UserID: "u1"}
	keyB := memory.UserKey{AppName: types.AppName("tenant-b", "assistant"), UserID: "u1"}

	if err := r.AddMemory(ctx, keyA, "tenant A's private note", []string{"x"}); err != nil {
		t.Fatalf("AddMemory for tenant A: %v", err)
	}

	got, err := r.ReadMemories(ctx, keyB, 10)
	if err != nil {
		t.Fatalf("ReadMemories for tenant B: %v", err)
	}
	for _, e := range got {
		if e.Memory != nil && e.Memory.Memory == "tenant A's private note" {
			t.Fatal("tenant B read tenant A's memory: isolation is broken")
		}
	}

	// And tenant A can still read its own.
	own, err := r.ReadMemories(ctx, keyA, 10)
	if err != nil {
		t.Fatalf("ReadMemories for tenant A: %v", err)
	}
	if len(own) == 0 {
		t.Fatal("tenant A cannot read the memory it just wrote")
	}
}

func TestMissingDefaultBackendFailsAtStartup(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.Backends = []config.BackendConfig{{Name: "redis-main", Kind: KindInMemory}}
	cfg.Storage.Defaults = map[string]string{
		string(types.DataTypeMemory): "does-not-exist",
	}

	// Failing at startup rather than on the first memory call: a
	// configuration typo should stop the process, not surface later as an
	// error on one tenant's conversation.
	_, err := NewMemoryRouter(context.Background(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("a default naming an unregistered backend should fail at startup")
	}
}

func TestUnsupportedBackendKindIsRejected(t *testing.T) {
	cfg := &config.Config{}
	cfg.Storage.Backends = []config.BackendConfig{{Name: "weird", Kind: "cassandra"}}
	cfg.Storage.Defaults = map[string]string{
		string(types.DataTypeMemory): "weird",
	}

	_, err := NewMemoryRouter(context.Background(), cfg,
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err == nil {
		t.Fatal("an unsupported backend kind should fail rather than be skipped")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	r := testRouter(t, "redis-main")

	// Shutdown paths call Close from a defer and sometimes explicitly. A
	// second call must not panic or double-close a backend.
	if err := r.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
