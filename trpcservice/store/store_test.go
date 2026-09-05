package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests run against a real MySQL because their whole point is proving
// the SQL works — unique keys, JSON columns and transactional sequence
// allocation cannot be exercised against a mock.
//
//	TEST_MYSQL_DSN='root:pass@tcp(127.0.0.1:3306)/trpc_agent_platform?parseTime=true' \
//	  go test ./trpcservice/store/
func testStore(t *testing.T) *Store {
	t.Helper()
	dsn := os.Getenv("TEST_MYSQL_DSN")
	if dsn == "" {
		t.Skip("TEST_MYSQL_DSN not set; skipping store integration tests")
	}
	s, err := Open(context.Background(), config.MySQLConfig{
		DSN: dsn, MaxOpenConns: 4, MaxIdleConns: 2,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestChannelBindingByWebhook(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	b, err := s.ChannelBindingByWebhook(ctx, "/webhook/mock/demo")
	if err != nil {
		t.Fatalf("ChannelBindingByWebhook: %v", err)
	}

	if b.ChannelBindingID != "cb-demo" || b.TenantID != "tenant-demo" || b.AgentAppID != "assistant" {
		t.Errorf("unexpected binding: %+v", b)
	}
	// Capabilities drive branching in the main flow, so a JSON decode failure
	// here would silently disable long-message splitting and rate limiting.
	if b.Capabilities.InboundMode != types.InboundModePayload {
		t.Errorf("InboundMode = %q", b.Capabilities.InboundMode)
	}
	if !b.Capabilities.SupportsPush {
		t.Error("SupportsPush should be true; ACK-then-push depends on it")
	}
	if b.Capabilities.MaxTextLength != 2048 {
		t.Errorf("MaxTextLength = %d", b.Capabilities.MaxTextLength)
	}
}

func TestChannelBindingUnknownPathIsNotFound(t *testing.T) {
	s := testStore(t)
	// An unknown path must stop the request. Returning a zero binding would
	// leave the message with no tenant.
	_, err := s.ChannelBindingByWebhook(context.Background(), "/webhook/does/not/exist")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

func TestTenantByID(t *testing.T) {
	s := testStore(t)
	tenant, err := s.TenantByID(context.Background(), "tenant-demo")
	if err != nil {
		t.Fatalf("TenantByID: %v", err)
	}
	if !tenant.Active() {
		t.Errorf("tenant not active: %+v", tenant)
	}
	if tenant.Settings == nil {
		t.Error("settings JSON did not decode")
	}
}

func TestDeploymentAndVersionPick(t *testing.T) {
	s := testStore(t)
	d, err := s.Deployment(context.Background(), "tenant-demo", "assistant", "prod")
	if err != nil {
		t.Fatalf("Deployment: %v", err)
	}

	if got := d.TotalWeight(); got != 100 {
		t.Errorf("TotalWeight = %d, want 100", got)
	}
	v, err := d.PickVersion(0)
	if err != nil {
		t.Fatalf("PickVersion: %v", err)
	}
	if v != "v1" {
		t.Errorf("PickVersion = %q, want v1", v)
	}
}

func TestRuntimeSpecLoadsVersionAndBindings(t *testing.T) {
	s := testStore(t)
	key := types.RuntimeKey{TenantID: "tenant-demo", AgentAppID: "assistant", AgentVersion: "v1"}

	spec, err := s.RuntimeSpec(context.Background(), key)
	if err != nil {
		t.Fatalf("RuntimeSpec: %v", err)
	}

	if spec.ModelName != "deepseek-chat" {
		t.Errorf("ModelName = %q", spec.ModelName)
	}
	if spec.SystemPrompt == "" {
		t.Error("SystemPrompt empty")
	}
	if spec.ModelAPIKeyRef == "" {
		t.Error("ModelAPIKeyRef should hold a secret:// reference")
	}
	if len(spec.Tools) != 2 {
		t.Fatalf("got %d tools, want 2 (search, calculator)", len(spec.Tools))
	}
	for _, tb := range spec.Tools {
		if tb.Mode != types.ToolModeAllow {
			t.Errorf("tool %s mode = %q", tb.ToolName, tb.Mode)
		}
	}
	if len(spec.Extensions) != 1 {
		t.Fatalf("got %d extensions, want 1", len(spec.Extensions))
	}
	if spec.Extensions[0].Kind != types.ExtensionKindCallback {
		t.Errorf("extension kind = %q", spec.Extensions[0].Kind)
	}
	// MCP and skills are deliberately empty in phase one, but assembly must
	// still iterate them so phase two is a data change rather than a code one.
	if len(spec.MCPServers) != 0 || len(spec.Skills) != 0 {
		t.Errorf("expected no MCP/skill bindings, got %d/%d", len(spec.MCPServers), len(spec.Skills))
	}
}

func TestRuntimeSpecRejectsIncompleteKey(t *testing.T) {
	s := testStore(t)
	// A key missing the tenant would, if allowed through, read another
	// tenant's configuration.
	_, err := s.RuntimeSpec(context.Background(), types.RuntimeKey{AgentAppID: "assistant", AgentVersion: "v1"})
	if err == nil {
		t.Fatal("want error for key without tenant")
	}
}

func TestFindOrCreateSessionFreezesVersionAndIsIdempotent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	binding, err := s.ChannelBindingByWebhook(ctx, "/webhook/mock/demo")
	if err != nil {
		t.Fatalf("binding: %v", err)
	}

	scopeKey := "user-" + uuid.NewString()
	t.Cleanup(func() {
		s.DB().Exec(`DELETE FROM session_events WHERE session_id IN
			(SELECT session_id FROM sessions WHERE scope_key = ?)`, scopeKey)
		s.DB().Exec(`DELETE FROM sessions WHERE scope_key = ?`, scopeKey)
	})

	lookup := SessionLookup{
		Binding: binding, Scope: types.ScopeSingle,
		ScopeKey: scopeKey, InternalUserID: scopeKey,
	}

	first, created, err := s.FindOrCreateSession(ctx, lookup)
	if err != nil {
		t.Fatalf("first FindOrCreateSession: %v", err)
	}
	if !created {
		t.Error("first call should have created the session")
	}
	if first.AgentVersion != "v1" {
		t.Errorf("AgentVersion = %q, want v1", first.AgentVersion)
	}

	second, created, err := s.FindOrCreateSession(ctx, lookup)
	if err != nil {
		t.Fatalf("second FindOrCreateSession: %v", err)
	}
	if created {
		t.Error("second call should have found the existing session")
	}
	if second.SessionID != first.SessionID {
		t.Errorf("session id changed: %s then %s", first.SessionID, second.SessionID)
	}
	// The frozen version is the whole point: a second message must not be
	// able to redraw and land on a different configuration.
	if second.AgentVersion != first.AgentVersion {
		t.Errorf("version changed between calls: %s then %s", first.AgentVersion, second.AgentVersion)
	}
}

func TestInsertInboundEventDeduplicates(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	eventID := "evt-" + uuid.NewString()
	t.Cleanup(func() {
		s.DB().Exec(`DELETE FROM inbound_events WHERE external_event_id = ?`, eventID)
	})

	ev := &types.InboundEvent{
		TenantID:         "tenant-demo",
		ChannelBindingID: "cb-demo",
		ExternalEventID:  eventID,
		RequestID:        "req-" + uuid.NewString(),
		TraceID:          "trace-" + uuid.NewString(),
		State:            types.StateProcessing,
		Payload:          &types.InboundMessage{Text: "hello", Channel: "mock"},
	}

	inserted, err := s.InsertInboundEvent(ctx, ev)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if !inserted {
		t.Fatal("first insert should report inserted")
	}

	// A redelivery of the same platform event must be reported as duplicate,
	// not as an error: the caller ACKs again without rerunning the agent.
	inserted, err = s.InsertInboundEvent(ctx, ev)
	if err != nil {
		t.Fatalf("second insert returned error instead of duplicate: %v", err)
	}
	if inserted {
		t.Error("second insert should report duplicate")
	}
}

func TestUpdateInboundStateAndMissingRow(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	eventID := "evt-" + uuid.NewString()
	t.Cleanup(func() {
		s.DB().Exec(`DELETE FROM inbound_events WHERE external_event_id = ?`, eventID)
	})

	ev := &types.InboundEvent{
		TenantID: "tenant-demo", ChannelBindingID: "cb-demo",
		ExternalEventID: eventID, RequestID: "req-1", State: types.StateProcessing,
	}
	if _, err := s.InsertInboundEvent(ctx, ev); err != nil {
		t.Fatalf("insert: %v", err)
	}

	if err := s.UpdateInboundState(ctx, "cb-demo", eventID, types.StateDeliveryFailed, "push timeout"); err != nil {
		t.Fatalf("UpdateInboundState: %v", err)
	}

	var state string
	var attempts int
	err := s.DB().QueryRow(
		`SELECT state, attempts FROM inbound_events WHERE channel_binding_id = ? AND external_event_id = ?`,
		"cb-demo", eventID).Scan(&state, &attempts)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if state != string(types.StateDeliveryFailed) {
		t.Errorf("state = %q", state)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}

	if err := s.UpdateInboundState(ctx, "cb-demo", "no-such-event", types.StateFailed, ""); !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound for missing row, got %v", err)
	}
}

func TestAppendSessionEventAllocatesSequence(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	binding, err := s.ChannelBindingByWebhook(ctx, "/webhook/mock/demo")
	if err != nil {
		t.Fatalf("binding: %v", err)
	}
	scopeKey := "user-" + uuid.NewString()
	t.Cleanup(func() {
		s.DB().Exec(`DELETE FROM session_events WHERE session_id IN
			(SELECT session_id FROM sessions WHERE scope_key = ?)`, scopeKey)
		s.DB().Exec(`DELETE FROM sessions WHERE scope_key = ?`, scopeKey)
	})

	sess, _, err := s.FindOrCreateSession(ctx, SessionLookup{
		Binding: binding, Scope: types.ScopeSingle, ScopeKey: scopeKey, InternalUserID: scopeKey,
	})
	if err != nil {
		t.Fatalf("session: %v", err)
	}

	for want := int64(1); want <= 3; want++ {
		got, err := s.AppendSessionEvent(ctx, &types.SessionEvent{
			TenantID:  sess.TenantID,
			SessionID: sess.SessionID,
			EventType: types.EventTypeUserMessage,
			Role:      "user",
			Content:   map[string]any{"text": "hi"},
			RequestID: "req-" + uuid.NewString(),
			TraceID:   "trace-1",
		})
		if err != nil {
			t.Fatalf("AppendSessionEvent %d: %v", want, err)
		}
		if got != want {
			t.Fatalf("sequence = %d, want %d", got, want)
		}
	}

	// last_sequence on the session must track the events, since it is what
	// the next allocation reads under lock.
	reloaded, err := s.SessionByID(ctx, sess.TenantID, sess.SessionID)
	if err != nil {
		t.Fatalf("SessionByID: %v", err)
	}
	if reloaded.LastSequence != 3 {
		t.Errorf("LastSequence = %d, want 3", reloaded.LastSequence)
	}
}
