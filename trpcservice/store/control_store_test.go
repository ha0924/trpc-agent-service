package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests cover the control-plane writes. Two properties matter most and
// neither can be checked without a real database:
//
//   - cross-tenant reach. Every statement carries its tenant_id, and the test
//     for that is naming another tenant's id and getting nothing back.
//   - version immutability. Enforced by `status = 'draft'` in the WHERE, so it
//     holds under concurrency; a check in Go would not.

// newTenant creates a throwaway tenant and registers its cleanup.
func newTenant(t *testing.T, s *Store) string {
	t.Helper()
	ctx := context.Background()
	id := "t-" + uuid.NewString()[:8]

	if err := s.CreateTenant(ctx, &types.Tenant{
		TenantID: id, Name: "test tenant", Status: types.StatusActive,
		Settings: map[string]any{"daily_token_budget": 1000},
	}); err != nil {
		t.Fatalf("CreateTenant: %v", err)
	}
	t.Cleanup(func() {
		s.db.ExecContext(ctx, `DELETE FROM audit_policies WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM agent_tool_bindings WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM agent_extension_bindings WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM channel_bindings WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM agent_versions WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM agent_apps WHERE tenant_id = ?`, id)
		s.db.ExecContext(ctx, `DELETE FROM tenants WHERE tenant_id = ?`, id)
	})
	return id
}

func newAgent(t *testing.T, s *Store, tenantID string) string {
	t.Helper()
	id := "a-" + uuid.NewString()[:8]
	if err := s.CreateAgentApp(context.Background(), &types.AgentApp{
		TenantID: tenantID, AgentAppID: id, Name: "test agent", Status: types.StatusActive,
	}); err != nil {
		t.Fatalf("CreateAgentApp: %v", err)
	}
	return id
}

func newDraft(t *testing.T, s *Store, tenantID, agentID, version string) types.RuntimeKey {
	t.Helper()
	if err := s.CreateAgentVersion(context.Background(), &types.AgentVersion{
		TenantID: tenantID, AgentAppID: agentID, Version: version,
		ModelName: "deepseek-chat", SystemPrompt: "you are a test",
		ModelParams: map[string]any{"temperature": 0.5},
	}); err != nil {
		t.Fatalf("CreateAgentVersion: %v", err)
	}
	return types.RuntimeKey{TenantID: tenantID, AgentAppID: agentID, AgentVersion: version}
}

func TestCreateTenantRejectsDuplicate(t *testing.T) {
	s := testStore(t)
	id := newTenant(t, s)

	err := s.CreateTenant(context.Background(), &types.Tenant{
		TenantID: id, Name: "again", Status: types.StatusActive,
	})
	// ErrDuplicate rather than a generic failure: the API answers 409 on this,
	// and "the id is taken" needs a different reaction from "the database is
	// down".
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second CreateTenant error = %v, want ErrDuplicate", err)
	}
}

func TestCreateAgentRequiresExistingTenant(t *testing.T) {
	s := testStore(t)

	// The schema has no foreign keys — deliberately, so a tenant's data can be
	// archived without cross-table constraints blocking it. That moves this
	// check to the store layer, so it needs a test.
	err := s.CreateAgentApp(context.Background(), &types.AgentApp{
		TenantID: "t-does-not-exist", AgentAppID: "a1",
		Name: "orphan", Status: types.StatusActive,
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("CreateAgentApp for missing tenant = %v, want ErrNotFound", err)
	}
}

func TestAgentIsScopedToItsTenant(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenantA, tenantB := newTenant(t, s), newTenant(t, s)
	agentA := newAgent(t, s, tenantA)

	// Naming tenant A's agent id under tenant B must find nothing. If this
	// ever passes, one tenant can attach versions to another's agent — the
	// worst failure this platform can have.
	err := s.CreateAgentVersion(ctx, &types.AgentVersion{
		TenantID: tenantB, AgentAppID: agentA, Version: "v1", ModelName: "m",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant version create = %v, want ErrNotFound", err)
	}
}

func TestPublishedVersionIsImmutable(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	key := newDraft(t, s, tenant, agent, "v1")

	// A draft is editable.
	if err := s.UpdateDraftVersion(ctx, &types.AgentVersion{
		TenantID: tenant, AgentAppID: agent, Version: "v1",
		ModelName: "deepseek-chat", SystemPrompt: "edited",
	}); err != nil {
		t.Fatalf("UpdateDraftVersion on a draft: %v", err)
	}

	if err := s.PublishVersion(ctx, key); err != nil {
		t.Fatalf("PublishVersion: %v", err)
	}

	// After publishing it is not. This is what keeps a cached Runtime from
	// ever being stale: the configuration behind a version cannot change.
	err := s.UpdateDraftVersion(ctx, &types.AgentVersion{
		TenantID: tenant, AgentAppID: agent, Version: "v1",
		ModelName: "deepseek-chat", SystemPrompt: "sneaky edit",
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("editing a published version = %v, want ErrNotFound", err)
	}
}

func TestPublishTwiceIsRejected(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	key := newDraft(t, s, tenant, agent, "v1")

	if err := s.PublishVersion(ctx, key); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	// The second publish must not silently reset published_at — that would
	// lose the date the version actually went live.
	if err := s.PublishVersion(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second publish = %v, want ErrNotFound", err)
	}
}

func TestBindingsOfPublishedVersionAreFrozen(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	key := newDraft(t, s, tenant, agent, "v1")

	tools := []types.ToolBinding{
		{ToolName: "calculator", Mode: types.ToolModeAllow},
		{ToolName: "search", Mode: types.ToolModeDeny},
	}
	if err := s.ReplaceToolBindings(ctx, key, tools); err != nil {
		t.Fatalf("ReplaceToolBindings on a draft: %v", err)
	}

	spec, err := s.RuntimeSpec(ctx, key)
	if err != nil {
		t.Fatalf("RuntimeSpec: %v", err)
	}
	if len(spec.Tools) != 2 {
		t.Fatalf("spec has %d tools, want 2", len(spec.Tools))
	}

	if err := s.PublishVersion(ctx, key); err != nil {
		t.Fatalf("PublishVersion: %v", err)
	}

	// A published version's permission grant is frozen. Allowing a change here
	// would be a privilege escalation on a version already serving traffic.
	if err := s.ReplaceToolBindings(ctx, key, nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replacing tools on a published version = %v, want ErrNotFound", err)
	}
}

func TestReplaceToolBindingsIsWholesale(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	key := newDraft(t, s, tenant, agent, "v1")

	if err := s.ReplaceToolBindings(ctx, key, []types.ToolBinding{
		{ToolName: "calculator", Mode: types.ToolModeAllow},
		{ToolName: "search", Mode: types.ToolModeAllow},
	}); err != nil {
		t.Fatalf("first replace: %v", err)
	}

	// Replacement, not merge: the list *is* the grant, so a shorter list must
	// actually revoke. A merge would make revocation impossible.
	if err := s.ReplaceToolBindings(ctx, key, []types.ToolBinding{
		{ToolName: "calculator", Mode: types.ToolModeAllow},
	}); err != nil {
		t.Fatalf("second replace: %v", err)
	}

	spec, err := s.RuntimeSpec(ctx, key)
	if err != nil {
		t.Fatalf("RuntimeSpec: %v", err)
	}
	if len(spec.Tools) != 1 || spec.Tools[0].ToolName != "calculator" {
		t.Fatalf("tools = %+v, want only calculator", spec.Tools)
	}
}

func TestArchiveOnlyAppliesToPublished(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	key := newDraft(t, s, tenant, agent, "v1")

	// A draft is not archivable: archiving is how a *published* version is
	// retired, and letting it apply to drafts would hide typos in the version
	// name behind a success response.
	if err := s.ArchiveVersion(ctx, key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("archiving a draft = %v, want ErrNotFound", err)
	}

	if err := s.PublishVersion(ctx, key); err != nil {
		t.Fatalf("PublishVersion: %v", err)
	}
	if err := s.ArchiveVersion(ctx, key); err != nil {
		t.Fatalf("ArchiveVersion: %v", err)
	}
}

func TestUpsertChannelBindingRoundTrips(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)
	bindingID := "cb-" + uuid.NewString()[:8]

	b := &types.ChannelBinding{
		ChannelBindingID: bindingID,
		TenantID:         tenant,
		AgentAppID:       agent,
		Env:              "prod",
		Channel:          "mock",
		WebhookPath:      "/webhook/mock/" + bindingID,
		SecretRef:        "secret://test/token",
		Capabilities: types.Capabilities{
			InboundMode: types.InboundModePayload, SupportsPush: true,
			MaxTextLength: 2048, RateLimitPerMin: 20,
		},
		Status: types.StatusActive,
	}
	if err := s.UpsertChannelBinding(ctx, b); err != nil {
		t.Fatalf("UpsertChannelBinding: %v", err)
	}

	// Re-applying identical configuration must not fail: deployment scripts
	// run repeatedly.
	if err := s.UpsertChannelBinding(ctx, b); err != nil {
		t.Fatalf("idempotent re-upsert: %v", err)
	}

	got, err := s.ChannelBindingByID(ctx, tenant, bindingID)
	if err != nil {
		t.Fatalf("ChannelBindingByID: %v", err)
	}
	if got.Capabilities.InboundMode != types.InboundModePayload {
		t.Errorf("InboundMode = %q, want payload", got.Capabilities.InboundMode)
	}
	if got.Capabilities.MaxTextLength != 2048 {
		t.Errorf("MaxTextLength = %d, want 2048", got.Capabilities.MaxTextLength)
	}
}

func TestStreamBindingsCoexistWithoutWebhookPath(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)
	agent := newAgent(t, s, tenant)

	// uk_webhook is a unique key and both of these have no path. MySQL allows
	// multiple NULLs, which is why the store writes NULL rather than "" —
	// several empty strings would collide and the second binding would fail.
	for i := 0; i < 2; i++ {
		id := "cb-stream-" + uuid.NewString()[:8]
		if err := s.UpsertChannelBinding(ctx, &types.ChannelBinding{
			ChannelBindingID: id, TenantID: tenant, AgentAppID: agent,
			Env: "prod", Channel: "wecom_aibot", SecretRef: "secret://x",
			Capabilities: types.Capabilities{InboundMode: types.InboundModeStream},
			Status:       types.StatusActive,
		}); err != nil {
			t.Fatalf("stream binding %d: %v", i, err)
		}
	}
}

func TestSetTenantStatusSuspends(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)

	if err := s.SetTenantStatus(ctx, tenant, types.StatusSuspended); err != nil {
		t.Fatalf("SetTenantStatus: %v", err)
	}
	got, err := s.TenantByID(ctx, tenant)
	if err != nil {
		t.Fatalf("TenantByID: %v", err)
	}
	if got.Active() {
		t.Fatal("tenant should not be active after suspension")
	}
}

func TestUpdateMissingRowReportsNotFound(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// Without the RowsAffected check an UPDATE against a missing row succeeds
	// silently and the API answers 200 for a change that never happened.
	err := s.UpdateTenant(ctx, &types.Tenant{
		TenantID: "t-missing-" + uuid.NewString()[:8],
		Name:     "ghost", Status: types.StatusActive,
	})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateTenant on missing row = %v, want ErrNotFound", err)
	}
}

func TestAuditPolicyDefaultsWhenAbsent(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()

	// A tenant with no policy row must not fall back to "retain everything":
	// shipping this feature would then start logging full message bodies for
	// every pre-existing tenant.
	p, err := s.AuditPolicy(ctx, "t-no-policy-"+uuid.NewString()[:8])
	if err != nil {
		t.Fatalf("AuditPolicy: %v", err)
	}
	if p.BodyMode != types.BodyTruncate {
		t.Errorf("default BodyMode = %q, want truncate", p.BodyMode)
	}
	if p.RedactLevel != types.RedactStandard {
		t.Errorf("default RedactLevel = %q, want standard", p.RedactLevel)
	}
}

func TestAuditPolicyRoundTrips(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)

	want := &types.AuditPolicy{
		TenantID: tenant, RedactLevel: types.RedactStrict,
		BodyMode: types.BodyHash, BodyMaxChars: 256, RetentionDays: 30,
	}
	if err := s.UpsertAuditPolicy(ctx, want); err != nil {
		t.Fatalf("UpsertAuditPolicy: %v", err)
	}

	got, err := s.AuditPolicy(ctx, tenant)
	if err != nil {
		t.Fatalf("AuditPolicy: %v", err)
	}
	if got.BodyMode != types.BodyHash || got.RedactLevel != types.RedactStrict {
		t.Fatalf("policy = %+v, want hash/strict", got)
	}
	if got.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", got.RetentionDays)
	}
}

func TestAuditPolicyRejectsInvalidValues(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)

	cases := []struct {
		name   string
		policy *types.AuditPolicy
	}{
		{"unknown redact level", &types.AuditPolicy{
			TenantID: tenant, RedactLevel: "wide-open",
			BodyMode: types.BodyTruncate, BodyMaxChars: 10, RetentionDays: 1}},
		{"unknown body mode", &types.AuditPolicy{
			TenantID: tenant, RedactLevel: types.RedactStandard,
			BodyMode: "keep-forever", BodyMaxChars: 10, RetentionDays: 1}},
		{"truncate without a limit", &types.AuditPolicy{
			TenantID: tenant, RedactLevel: types.RedactStandard,
			BodyMode: types.BodyTruncate, BodyMaxChars: 0, RetentionDays: 1}},
		{"zero retention", &types.AuditPolicy{
			TenantID: tenant, RedactLevel: types.RedactStandard,
			BodyMode: types.BodyTruncate, BodyMaxChars: 10, RetentionDays: 0}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// Rejected at write time, so one bad value cannot degrade every
			// audit record written afterwards.
			if err := s.UpsertAuditPolicy(ctx, c.policy); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestAuditPolicyTrimsRecordBody(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	tenant := newTenant(t, s)

	if err := s.UpsertAuditPolicy(ctx, &types.AuditPolicy{
		TenantID: tenant, RedactLevel: types.RedactStandard,
		BodyMode: types.BodyDrop, BodyMaxChars: 512, RetentionDays: 7,
	}); err != nil {
		t.Fatalf("UpsertAuditPolicy: %v", err)
	}

	rec := &types.AuditRecord{
		TenantID: tenant, RequestID: "req-" + uuid.NewString(),
		EventType: types.AuditToolCall, Decision: types.DecisionAllow,
		Reason:   "user said something private",
		ToolName: "calculator",
		Detail:   map[string]any{"text": "private text", "approval_id": "apr-123"},
	}

	// The policy is applied inside WriteAudit rather than at call sites: a
	// rule enforced by convention gets forgotten at the one site that matters.
	if err := s.WriteAudit(ctx, rec); err != nil {
		t.Fatalf("WriteAudit: %v", err)
	}
	t.Cleanup(func() {
		s.db.ExecContext(ctx, `DELETE FROM audit_logs WHERE tenant_id = ?`, tenant)
	})

	if rec.Reason != "" {
		t.Errorf("Reason = %q, want empty under body_mode=drop", rec.Reason)
	}
	if _, ok := rec.Detail["text"]; ok {
		t.Error("Detail[text] should be removed under body_mode=drop")
	}
	// Structural keys survive: dropping them would leave a record that says
	// something happened without saying what, and correlation would break.
	if rec.Detail["approval_id"] != "apr-123" {
		t.Errorf("Detail[approval_id] = %v, want it preserved", rec.Detail["approval_id"])
	}
	if rec.ToolName != "calculator" {
		t.Errorf("ToolName = %q, want it preserved", rec.ToolName)
	}
}
