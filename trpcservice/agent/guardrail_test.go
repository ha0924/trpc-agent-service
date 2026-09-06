// 设计依据：docs/治理监控与安全设计.md §7.2「治理策略」危险工具二次确认

package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests cover the approval gate without a database, because the
// properties that matter are about *sequencing*, not storage:
//
//   - a gated call is refused until an approval exists;
//   - one approval buys exactly one execution;
//   - an approval is bound to the arguments a human reviewed;
//   - an unreadable approval store refuses rather than proceeds.
//
// Each of those was broken or absent before: the guardrail used to record an
// approval id into an audit log and refuse forever, and the two store
// functions it needed had zero call sites.

// fakeApprovals is an in-memory ApprovalStore.
type fakeApprovals struct {
	mu sync.Mutex
	// approved is keyed by tenant|session|tool|fingerprint, matching the
	// claim's WHERE clause.
	approved map[string]int
	created  []*types.ToolApproval
	claimErr error
}

func newFakeApprovals() *fakeApprovals {
	return &fakeApprovals{approved: make(map[string]int)}
}

func approvalKey(tenant, session, toolName, fp string) string {
	return tenant + "|" + session + "|" + toolName + "|" + fp
}

func (f *fakeApprovals) CreateToolApproval(ctx context.Context, a *types.ToolApproval) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.created = append(f.created, a)
	return nil
}

func (f *fakeApprovals) ClaimToolApproval(
	ctx context.Context, tenantID, sessionID, toolName, fingerprint string,
) (bool, error) {
	if f.claimErr != nil {
		return false, f.claimErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	k := approvalKey(tenantID, sessionID, toolName, fingerprint)
	if f.approved[k] > 0 {
		// Decremented on claim: this is the in-memory equivalent of the
		// approved→consumed transition, and it is what makes one approval
		// buy one execution.
		f.approved[k]--
		return true, nil
	}
	return false, nil
}

// grant marks one approval available.
func (f *fakeApprovals) grant(tenant, session, toolName, fp string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.approved[approvalKey(tenant, session, toolName, fp)]++
}

func (f *fakeApprovals) createdCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.created)
}

var _ ApprovalStore = (*fakeApprovals)(nil)

// gateHarness builds the guardrail and exposes the registered hook.
type gateHarness struct {
	hook      func(context.Context, *tool.BeforeToolArgs) (*tool.BeforeToolResult, error)
	approvals *fakeApprovals
	ctx       context.Context
}

func newGate(t *testing.T, approvals ApprovalStore) *gateHarness {
	t.Helper()

	callbacks := tool.NewCallbacks()
	mp := &MountPoints{
		Tool: callbacks,
		Spec: &types.RuntimeSpec{
			Tools: []types.ToolBinding{
				{ToolName: "delete_order", Mode: types.ToolModeAsk},
				{ToolName: "calculator", Mode: types.ToolModeAllow},
			},
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
		Deps:   PolicyDeps{Approvals: approvals},
	}

	if err := dangerousToolApproval(types.ExtensionBinding{}, mp); err != nil {
		t.Fatalf("dangerousToolApproval: %v", err)
	}

	rc := &types.RequestContext{
		TenantID: "tenant-test", AgentAppID: "agent-1",
		SessionID: "sess-1", RequestID: "req-1", UserID: "user-1",
	}
	h := &gateHarness{ctx: types.NewContext(context.Background(), rc)}
	if fa, ok := approvals.(*fakeApprovals); ok {
		h.approvals = fa
	}

	// Reach the registered hook through the callbacks themselves, so the test
	// exercises the same path the framework uses.
	h.hook = callbacks.RunBeforeTool
	return h
}

func callTool(t *testing.T, h *gateHarness, name, argsJSON string) *tool.BeforeToolResult {
	t.Helper()
	res, err := h.hook(h.ctx, &tool.BeforeToolArgs{
		ToolName:  name,
		Arguments: []byte(argsJSON),
	})
	if err != nil {
		t.Fatalf("hook returned an error: %v", err)
	}
	return res
}

// awaiting reports whether the result is a hold-for-confirmation.
func awaiting(res *tool.BeforeToolResult) bool {
	if res == nil {
		return false
	}
	m, ok := res.CustomResult.(map[string]any)
	if !ok {
		return false
	}
	return m["status"] == "awaiting_approval"
}

func refused(res *tool.BeforeToolResult) bool {
	if res == nil {
		return false
	}
	m, ok := res.CustomResult.(map[string]any)
	if !ok {
		return false
	}
	return m["status"] == "refused"
}

func TestUngatedToolPassesThrough(t *testing.T) {
	h := newGate(t, newFakeApprovals())

	// A tool in allow mode must not touch the approval path at all.
	if res := callTool(t, h, "calculator", `{"a":1}`); res != nil {
		t.Fatalf("calculator should pass through untouched, got %+v", res)
	}
	if h.approvals.createdCount() != 0 {
		t.Error("an allow-mode tool must not create an approval")
	}
}

func TestFirstCallRecordsAndRefuses(t *testing.T) {
	h := newGate(t, newFakeApprovals())

	res := callTool(t, h, "delete_order", `{"id":123}`)
	if !awaiting(res) {
		t.Fatalf("first call should be held for confirmation, got %+v", res)
	}
	// The request must be durable before the refusal: an approval nobody can
	// find is an approval nobody can grant.
	if h.approvals.createdCount() != 1 {
		t.Fatalf("created %d approvals, want 1", h.approvals.createdCount())
	}

	got := h.approvals.created[0]
	if got.State != types.ApprovalPending {
		t.Errorf("state = %q, want pending", got.State)
	}
	if got.ArgsFingerprint == "" {
		t.Error("the approval must carry a fingerprint, or it would authorise any arguments")
	}
	if got.ExpiresAt == nil {
		t.Error("the approval must expire, or an unanswered request authorises forever")
	}
}

func TestApprovedCallRunsOnce(t *testing.T) {
	fa := newFakeApprovals()
	h := newGate(t, fa)
	args := `{"id":123}`
	fp := types.FingerprintArgs([]byte(args))

	fa.grant("tenant-test", "sess-1", "delete_order", fp)

	// nil means "let it through to the real tool".
	if res := callTool(t, h, "delete_order", args); res != nil {
		t.Fatalf("an approved call should proceed, got %+v", res)
	}

	// The second call must be held again. Without the consumed transition,
	// approving once would permanently disable the gate — worse than no gate,
	// because it still looks like one.
	if res := callTool(t, h, "delete_order", args); !awaiting(res) {
		t.Fatalf("the approval must be spent after one execution, got %+v", res)
	}
}

func TestApprovalIsBoundToItsArguments(t *testing.T) {
	fa := newFakeApprovals()
	h := newGate(t, fa)

	approvedArgs := `{"id":123}`
	fa.grant("tenant-test", "sess-1", "delete_order",
		types.FingerprintArgs([]byte(approvedArgs)))

	// Approving "delete order 123" must not authorise "delete order 999".
	// This is the difference between confirming an action and switching the
	// gate off for the tool.
	if res := callTool(t, h, "delete_order", `{"id":999}`); !awaiting(res) {
		t.Fatalf("different arguments must not reuse an approval, got %+v", res)
	}

	// The originally approved arguments still work.
	if res := callTool(t, h, "delete_order", approvedArgs); res != nil {
		t.Fatalf("the approved arguments should proceed, got %+v", res)
	}
}

func TestClaimFailureRefusesRatherThanProceeds(t *testing.T) {
	fa := newFakeApprovals()
	fa.claimErr = errors.New("database unreachable")
	h := newGate(t, fa)

	// Unknown whether an approval exists. Refusing is the only safe reading:
	// proceeding could run a dangerous tool nobody approved, while refusing
	// costs one retry.
	res := callTool(t, h, "delete_order", `{"id":123}`)
	if !refused(res) {
		t.Fatalf("a failed claim must refuse, got %+v", res)
	}
}

func TestNoApprovalStoreRefusesEveryTime(t *testing.T) {
	h := newGate(t, nil)

	// With no store nothing can ever be approved, so the tool is effectively
	// disabled. That is safe, and the assembler logs it — but it must not
	// silently pass.
	if res := callTool(t, h, "delete_order", `{"id":123}`); !awaiting(res) {
		t.Fatalf("without an approval store the call must be held, got %+v", res)
	}
}

func TestFingerprintDistinguishesArguments(t *testing.T) {
	a := types.FingerprintArgs([]byte(`{"id":1}`))
	b := types.FingerprintArgs([]byte(`{"id":2}`))
	if a == b {
		t.Fatal("different arguments must fingerprint differently")
	}
	if a != types.FingerprintArgs([]byte(`{"id":1}`)) {
		t.Fatal("the same arguments must fingerprint identically, or an approval could never be claimed")
	}
	// "no arguments" is distinct from a hash, so the two cannot be confused.
	if types.FingerprintArgs(nil) != "none" {
		t.Errorf("empty arguments = %q, want a distinct sentinel", types.FingerprintArgs(nil))
	}
}

func TestApprovalStateHelpers(t *testing.T) {
	cases := []struct {
		state    types.ApprovalState
		decided  bool
		terminal bool
	}{
		{types.ApprovalPending, false, false},
		{types.ApprovalApproved, true, false},
		{types.ApprovalRejected, true, true},
		{types.ApprovalExpired, false, true},
		{types.ApprovalConsumed, false, true},
	}
	for _, c := range cases {
		t.Run(string(c.state), func(t *testing.T) {
			if got := c.state.Decided(); got != c.decided {
				t.Errorf("Decided() = %v, want %v", got, c.decided)
			}
			if got := c.state.Terminal(); got != c.terminal {
				t.Errorf("Terminal() = %v, want %v", got, c.terminal)
			}
		})
	}
}
