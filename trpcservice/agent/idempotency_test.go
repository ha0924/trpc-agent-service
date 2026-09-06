package agent

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"trpc.group/trpc-go/trpc-agent-go/tool"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// These tests cover the operation key, which closes a gap between the design
// and the code: 风险清单 #2 named it as the first mitigation for "有副作用
// Tool 被重复执行" while it existed only in prose.
//
// The properties that matter are all about *sameness and difference*:
//
//   - the same request retried must produce the same key, or deduplication
//     downstream matches nothing;
//   - two different calls in one request must produce different keys, or
//     "book two tickets" collapses into one;
//   - a tool that does not declare side effects must be left untouched, or
//     its arguments stop matching its schema and the call fails.

func newIdempotencyGate(t *testing.T, tools []types.ToolBinding) (
	func(context.Context, *tool.BeforeToolArgs) (*tool.BeforeToolResult, error), bool,
) {
	t.Helper()
	callbacks := tool.NewCallbacks()
	mp := &MountPoints{
		Tool:   callbacks,
		Spec:   &types.RuntimeSpec{Tools: tools},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	if err := operationIdempotency(types.ExtensionBinding{}, mp); err != nil {
		t.Fatalf("operationIdempotency: %v", err)
	}
	// Reports whether a hook was registered at all: with no side-effect tool
	// the extension must not add one.
	return callbacks.RunBeforeTool, len(callbacks.BeforeTool) > 0
}

func requestCtx(requestID string) context.Context {
	ctx := types.NewContext(context.Background(), &types.RequestContext{
		TenantID: "tenant-test", AgentAppID: "agent-1",
		SessionID: "sess-1", RequestID: requestID,
	})
	return types.WithOperationCounter(ctx, types.NewOperationCounter())
}

// callAndReadKey invokes the hook and returns the injected key, if any.
func callAndReadKey(t *testing.T, hook func(context.Context, *tool.BeforeToolArgs) (*tool.BeforeToolResult, error),
	ctx context.Context, toolName, argsJSON string,
) (string, *tool.BeforeToolResult) {
	t.Helper()
	args := &tool.BeforeToolArgs{ToolName: toolName, Arguments: []byte(argsJSON)}
	res, err := hook(ctx, args)
	if err != nil {
		t.Fatalf("hook error: %v", err)
	}
	var fields map[string]any
	if err := json.Unmarshal(args.Arguments, &fields); err != nil {
		t.Fatalf("arguments are no longer valid JSON: %v", err)
	}
	key, _ := fields[types.OperationKeyField()].(string)
	return key, res
}

func TestNoHookWhenNoToolDeclaresSideEffects(t *testing.T) {
	_, registered := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "calculator", Mode: types.ToolModeAllow},
	})
	// Adding a callback that always passes costs a hop on every tool call for
	// nothing.
	if registered {
		t.Error("no side-effect tool should mean no hook is registered")
	}
}

func TestReadOnlyToolIsLeftUntouched(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Mode: types.ToolModeAllow,
			Params: map[string]any{"side_effect": true}},
		{ToolName: "calculator", Mode: types.ToolModeAllow},
	})

	// Injection is opt-in. Adding an unexpected field to a tool that does not
	// declare one would make the arguments stop matching its schema, turning
	// a safety feature into a failed call.
	key, res := callAndReadKey(t, hook, requestCtx("req-1"), "calculator", `{"a":1,"b":2}`)
	if key != "" {
		t.Errorf("calculator received an operation key %q; it declares no side effects", key)
	}
	if res != nil {
		t.Errorf("calculator should pass through untouched, got %+v", res)
	}
}

func TestSideEffectToolReceivesAKey(t *testing.T) {
	hook, registered := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Mode: types.ToolModeAllow,
			Params: map[string]any{"side_effect": true}},
	})
	if !registered {
		t.Fatal("a side-effect tool should register the hook")
	}

	key, res := callAndReadKey(t, hook, requestCtx("req-1"), "refund", `{"order":"A1"}`)
	if res != nil {
		t.Fatalf("the call should proceed, got %+v", res)
	}
	if key == "" {
		t.Fatal("a side-effect tool must receive an operation key")
	}
}

func TestOriginalArgumentsSurviveInjection(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	args := &tool.BeforeToolArgs{
		ToolName:  "refund",
		Arguments: []byte(`{"order":"A1","amount":42.5,"nested":{"x":1}}`),
	}
	if _, err := hook(requestCtx("req-1"), args); err != nil {
		t.Fatalf("hook: %v", err)
	}

	var fields map[string]any
	if err := json.Unmarshal(args.Arguments, &fields); err != nil {
		t.Fatalf("arguments are no longer valid JSON: %v", err)
	}
	// Decoding and re-encoding must not lose the model's own arguments.
	if fields["order"] != "A1" {
		t.Errorf("order = %v, want A1", fields["order"])
	}
	if fields["amount"] != 42.5 {
		t.Errorf("amount = %v, want 42.5", fields["amount"])
	}
	if _, ok := fields["nested"]; !ok {
		t.Error("nested object was dropped")
	}
}

func TestSameRequestAndCallProduceTheSameKey(t *testing.T) {
	// A retry of one message must produce the same key, or the downstream
	// system deduplicates against nothing and the effect happens twice.
	//
	// Two separate runs of the same request, each with a fresh counter, stand
	// in for the original attempt and its redelivery.
	hook1, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})
	first, _ := callAndReadKey(t, hook1, requestCtx("req-same"), "refund", `{"order":"A1"}`)

	hook2, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})
	second, _ := callAndReadKey(t, hook2, requestCtx("req-same"), "refund", `{"order":"A1"}`)

	if first == "" || first != second {
		t.Fatalf("a retried request must reuse its key: %q vs %q", first, second)
	}
}

func TestDifferentRequestsProduceDifferentKeys(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	a, _ := callAndReadKey(t, hook, requestCtx("req-a"), "refund", `{"order":"A1"}`)
	b, _ := callAndReadKey(t, hook, requestCtx("req-b"), "refund", `{"order":"A1"}`)

	// Two users refunding the same order are two operations. Sharing a key
	// would silently drop the second.
	if a == b {
		t.Fatalf("different requests must not share a key: both %q", a)
	}
}

func TestTwoCallsInOneRequestProduceDifferentKeys(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "book", Params: map[string]any{"side_effect": true}},
	})
	ctx := requestCtx("req-1")

	// "Book two tickets" is two operations within one request. The sequence
	// counter is what keeps them apart.
	first, _ := callAndReadKey(t, hook, ctx, "book", `{"seat":"1A"}`)
	second, _ := callAndReadKey(t, hook, ctx, "book", `{"seat":"1B"}`)

	if first == second {
		t.Fatalf("two calls in one request must not share a key: both %q", first)
	}
}

func TestDifferentArgumentsProduceDifferentKeys(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	// Position alone is not enough: an agent re-running after a partial
	// failure may issue its calls in a different order, so the arguments are
	// part of what identifies the operation.
	a, _ := callAndReadKey(t, hook, requestCtx("req-1"), "refund", `{"order":"A1"}`)
	b, _ := callAndReadKey(t, hook, requestCtx("req-1"), "refund", `{"order":"B2"}`)

	if a == b {
		t.Fatalf("different arguments must not share a key: both %q", a)
	}
}

func TestModelSuppliedKeyIsOverwritten(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	// A model that invented its own key could defeat deduplication by varying
	// it between retries. The key must come from the platform, which is the
	// only party that knows a call is a repeat.
	key, _ := callAndReadKey(t, hook, requestCtx("req-1"), "refund",
		`{"order":"A1","operation_key":"model-made-this-up"}`)

	if key == "model-made-this-up" {
		t.Fatal("a model-supplied key must be overwritten by the platform's")
	}
	if key == "" {
		t.Fatal("the platform's key should still be present")
	}
}

func TestMissingRequestIDRefusesTheCall(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	// No request id means nothing stable to key on, so any generated value
	// would differ between retries and protect nothing. Refusing costs a
	// failed turn; proceeding could cost a double refund.
	ctx := types.WithOperationCounter(context.Background(), types.NewOperationCounter())
	_, res := callAndReadKey(t, hook, ctx, "refund", `{"order":"A1"}`)

	if res == nil {
		t.Fatal("a side-effect call with no request id must be refused")
	}
	m, ok := res.CustomResult.(map[string]any)
	if !ok || m["error"] != "no_operation_key" {
		t.Fatalf("result = %+v, want a no_operation_key refusal", res.CustomResult)
	}
}

func TestMalformedArgumentsRefuseRatherThanProceed(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "refund", Params: map[string]any{"side_effect": true}},
	})

	// The key cannot be injected into arguments that will not parse.
	// Proceeding would run an unprotected side effect.
	args := &tool.BeforeToolArgs{
		ToolName:  "refund",
		Arguments: []byte(`{not valid json`),
	}
	res, err := hook(requestCtx("req-1"), args)
	if err != nil {
		t.Fatalf("hook should refuse via a result, not an error: %v", err)
	}
	if res == nil {
		t.Fatal("malformed arguments must be refused")
	}
}

func TestEmptyArgumentsStillGetAKey(t *testing.T) {
	hook, _ := newIdempotencyGate(t, []types.ToolBinding{
		{ToolName: "reset", Params: map[string]any{"side_effect": true}},
	})

	// A no-argument side effect — "reset the counter" — still needs a key.
	key, res := callAndReadKey(t, hook, requestCtx("req-1"), "reset", ``)
	if res != nil {
		t.Fatalf("the call should proceed, got %+v", res)
	}
	if key == "" {
		t.Fatal("an argument-less side effect still needs an operation key")
	}
}

func TestOperationKeyValidity(t *testing.T) {
	if (types.OperationKey{}).Valid() {
		t.Error("a key with no request id must not be valid")
	}
	if (types.OperationKey{}).String() != "" {
		t.Error("an invalid key must render empty rather than to a generated value")
	}

	k := types.OperationKey{RequestID: "req-1", ToolName: "t", Sequence: 1}
	if !k.Valid() {
		t.Error("a key with a request id should be valid")
	}
	if got := k.String(); len(got) < 4 || got[:3] != "op-" {
		t.Errorf("String() = %q, want an op- prefix so it is recognisable in logs", got)
	}
}

func TestOperationCounterStartsAtOne(t *testing.T) {
	c := types.NewOperationCounter()
	// Zero is reserved for "no counter", so the first real call must be 1 —
	// otherwise a missing counter and the first call would be
	// indistinguishable.
	if got := c.Next(); got != 1 {
		t.Errorf("first Next() = %d, want 1", got)
	}
	if got := c.Next(); got != 2 {
		t.Errorf("second Next() = %d, want 2", got)
	}

	var nilCounter *types.OperationCounter
	if got := nilCounter.Next(); got != 0 {
		t.Errorf("nil counter Next() = %d, want 0", got)
	}
}
