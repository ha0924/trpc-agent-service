package types

import (
	"context"
	"errors"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/model"
	"trpc.group/trpc-go/trpc-agent-go/openclaw/gwproto"
)

func TestRequestContextRoundTrip(t *testing.T) {
	want := &RequestContext{
		TenantID:     "tenant-demo",
		AgentAppID:   "assistant",
		AgentVersion: "v1",
		SessionID:    "sess-1",
		RequestID:    "req-1",
		TraceID:      "trace-1",
	}

	ctx := NewContext(context.Background(), want)
	got, err := FromContext(ctx)
	if err != nil {
		t.Fatalf("FromContext: %v", err)
	}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}

func TestFromContextMissingFailsClosed(t *testing.T) {
	// A context with no tenant must be an error, never a zero-value tenant:
	// silently treating an unknown tenant as "" is how cross-tenant leaks
	// happen.
	if _, err := FromContext(context.Background()); !errors.Is(err, ErrNoRequestContext) {
		t.Fatalf("want ErrNoRequestContext, got %v", err)
	}
}

func TestSafeAccessorsTolerateMissingContext(t *testing.T) {
	// Logging and metrics must not panic on a context that lost its tenant.
	if got := TenantID(context.Background()); got != "" {
		t.Errorf("TenantID = %q, want empty", got)
	}
	if got := TraceID(context.Background()); got != "" {
		t.Errorf("TraceID = %q, want empty", got)
	}
}

func TestRuntimeKeyAlwaysCarriesTenant(t *testing.T) {
	rc := &RequestContext{TenantID: "t1", AgentAppID: "app", AgentVersion: "v1"}
	key := rc.RuntimeKey()

	if key.TenantID != "t1" {
		t.Fatalf("RuntimeKey dropped the tenant: %+v", key)
	}
	if !key.Valid() {
		t.Fatalf("key should be valid: %+v", key)
	}
	if got, want := key.String(), "t1/app/v1"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}

	// Two tenants using the same agent name and version must not collide.
	other := RuntimeKey{TenantID: "t2", AgentAppID: "app", AgentVersion: "v1"}
	if key == other {
		t.Error("keys from different tenants must differ")
	}
	if key.String() == other.String() {
		t.Error("key strings from different tenants must differ")
	}
}

func TestRuntimeKeyValid(t *testing.T) {
	cases := []struct {
		name string
		key  RuntimeKey
		want bool
	}{
		{"complete", RuntimeKey{"t", "a", "v"}, true},
		{"no tenant", RuntimeKey{"", "a", "v"}, false},
		{"no app", RuntimeKey{"t", "", "v"}, false},
		{"no version", RuntimeKey{"t", "a", ""}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := c.key.Valid(); got != c.want {
				t.Errorf("Valid() = %v, want %v", got, c.want)
			}
		})
	}
}

func TestToModelMessagePlainText(t *testing.T) {
	m := &InboundMessage{Text: "你好"}
	got := m.ToModelMessage()

	if got.Role != model.RoleUser {
		t.Errorf("Role = %v, want user", got.Role)
	}
	if got.Content != "你好" {
		t.Errorf("Content = %q", got.Content)
	}
	if len(got.ContentParts) != 0 {
		t.Errorf("plain text should not produce parts, got %d", len(got.ContentParts))
	}
}

func TestToModelMessageConvertsParts(t *testing.T) {
	text := "看这张图"
	m := &InboundMessage{
		Text: text,
		Parts: []gwproto.ContentPart{
			{Type: gwproto.PartTypeText, Text: &text},
			{Type: gwproto.PartTypeImage, Image: &gwproto.ImagePart{URL: "https://e.com/a.png"}},
			{Type: gwproto.PartTypeFile, File: &gwproto.FilePart{Filename: "s.pdf", Format: "application/pdf"}},
		},
	}

	got := m.ToModelMessage()
	if len(got.ContentParts) != 3 {
		t.Fatalf("got %d parts, want 3", len(got.ContentParts))
	}
	if got.ContentParts[0].Type != model.ContentTypeText {
		t.Errorf("part 0 type = %v", got.ContentParts[0].Type)
	}
	if got.ContentParts[1].Image == nil || got.ContentParts[1].Image.URL != "https://e.com/a.png" {
		t.Errorf("image part not carried through: %+v", got.ContentParts[1])
	}
	if got.ContentParts[2].File == nil || got.ContentParts[2].File.Name != "s.pdf" {
		t.Errorf("file part not carried through: %+v", got.ContentParts[2])
	}
}

func TestToModelMessageDegradesUnmappablePartsToText(t *testing.T) {
	// Location has no model-side equivalent. It must become text rather than
	// vanish, so the model still sees that the user sent something.
	m := &InboundMessage{
		Parts: []gwproto.ContentPart{
			{Type: gwproto.PartTypeLocation, Location: &gwproto.LocationPart{
				Latitude: 39.9, Longitude: 116.4, Name: "北京",
			}},
		},
	}

	got := m.ToModelMessage()
	if len(got.ContentParts) != 1 {
		t.Fatalf("got %d parts, want 1", len(got.ContentParts))
	}
	part := got.ContentParts[0]
	if part.Type != model.ContentTypeText || part.Text == nil {
		t.Fatalf("location should degrade to text, got %+v", part)
	}
	if *part.Text == "" {
		t.Error("degraded text is empty")
	}
}

func TestToModelMessageFallsBackWhenPartsUnusable(t *testing.T) {
	// Parts present but all malformed: fall back to the plain text form
	// rather than sending an empty message.
	m := &InboundMessage{
		Text:  "fallback",
		Parts: []gwproto.ContentPart{{Type: gwproto.PartTypeImage, Image: nil}},
	}

	got := m.ToModelMessage()
	if got.Content != "fallback" {
		t.Errorf("Content = %q, want fallback", got.Content)
	}
	if len(got.ContentParts) != 0 {
		t.Errorf("want no parts, got %d", len(got.ContentParts))
	}
}

func TestDeploymentTotalWeight(t *testing.T) {
	d := &Deployment{Routes: []VersionRoute{{"v1", 90}, {"v2", 10}}}
	if got := d.TotalWeight(); got != 100 {
		t.Errorf("TotalWeight = %d, want 100", got)
	}
}

func TestTenantAndVersionStatusHelpers(t *testing.T) {
	if (&Tenant{Status: StatusActive}).Active() != true {
		t.Error("active tenant reported inactive")
	}
	if (&Tenant{Status: StatusSuspended}).Active() != false {
		t.Error("suspended tenant reported active")
	}
	if (*Tenant)(nil).Active() != false {
		t.Error("nil tenant must not be active")
	}
	if (&AgentVersion{Status: VersionStatusDraft}).Published() != false {
		t.Error("draft version reported published")
	}
	if (&AgentVersion{Status: VersionStatusPublished}).Published() != true {
		t.Error("published version reported unpublished")
	}
}

func TestLogFieldsCoverTheJoinKeys(t *testing.T) {
	// trace_id spanning both processes is the one observability guarantee
	// phase one must meet, so it has to be in the standard log field set.
	rc := &RequestContext{TenantID: "t", TraceID: "tr", RequestID: "rq", SessionID: "s"}
	f := rc.LogFields()
	for _, k := range []string{"tenant_id", "trace_id", "request_id", "session_id"} {
		if _, ok := f[k]; !ok {
			t.Errorf("LogFields missing %q", k)
		}
	}
	if (*RequestContext)(nil).LogFields() != nil {
		t.Error("nil RequestContext should yield nil fields")
	}
}
