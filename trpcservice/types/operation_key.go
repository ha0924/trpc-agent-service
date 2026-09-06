// 设计依据：docs/风险清单.md #2「有副作用 Tool 被重复执行」
//                docs/治理监控与安全设计.md §7.2「治理策略」

package types

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// OperationKey identifies one intended side effect, so a downstream system can
// make repeated attempts take effect once.
//
// It closes a gap between the design and the code. 风险清单 #2 lists "有副作用
// Tool 被重复执行" as a high risk and names the operation key as its primary
// mitigation, but the key itself was never implemented — the guarantee existed
// only in prose. Everything else in that row was in place: audit before
// effect, no blind retry on uncertain results, uk_event at the front door.
//
// Why the platform must supply it rather than let each tool invent one:
//
//   - A tool cannot know it is a retry. From inside the call, the first
//     attempt and the fifth look identical; only the platform knows the
//     request has been replayed.
//   - The key has to be *stable across* retries and *distinct between*
//     genuinely different calls. Deriving it from the request id plus the
//     call's position in the conversation gives both, and needs no extra
//     state.
//
// What this does not do: it cannot make a tool idempotent. It gives the
// downstream system enough information to deduplicate, which is the most a
// platform can do from outside. A tool that ignores the key stays unsafe, and
// that is a property of the tool, not of this mechanism.
type OperationKey struct {
	// RequestID is the inbound message being processed. Stable across every
	// retry of that message, which is what makes the key survive a redelivery.
	RequestID string

	// ToolName plus Sequence distinguish calls within one request. A single
	// agent turn may call the same tool twice with different arguments — "book
	// two tickets" — and those are different operations that must not
	// deduplicate against each other.
	ToolName string
	Sequence int

	// ArgsFingerprint guards against a subtler case: an agent that re-runs
	// after a partial failure may issue its calls in a different order, so
	// position alone can collide. Including the arguments means a key
	// identifies *what* was asked, not just when.
	ArgsFingerprint string
}

// String renders the key passed to the tool.
//
// Hashed rather than concatenated verbatim: the parts include a request id and
// an argument digest, and a downstream system will store this value, log it and
// possibly return it in an error. A fixed-width opaque token is safer to hand
// out and simpler for the receiver to index.
//
// The prefix keeps it recognisable in a log or a database column — an operator
// seeing it should be able to tell what kind of value it is.
func (k OperationKey) String() string {
	if k.RequestID == "" {
		// Without a request id there is nothing stable to key on, so a
		// generated value would defeat the purpose: every retry would get a
		// different key and deduplicate against nothing. Returning empty lets
		// the caller decide, and the caller refuses.
		return ""
	}
	raw := fmt.Sprintf("%s|%s|%d|%s",
		k.RequestID, k.ToolName, k.Sequence, k.ArgsFingerprint)
	sum := sha256.Sum256([]byte(raw))
	return "op-" + hex.EncodeToString(sum[:16])
}

// Valid reports whether the key can identify an operation.
func (k OperationKey) Valid() bool { return k.RequestID != "" }

// operationKeyField is the JSON field an operation key is injected into.
//
// A conventional name rather than a per-tool setting: a tool that accepts an
// operation key is agreeing to a platform convention, and letting each one
// choose a field name would mean the platform has to know every tool's schema
// to inject it.
const operationKeyField = "operation_key"

// OperationKeyField exposes the field name so tools can declare it and tests
// can assert on it without duplicating the literal.
func OperationKeyField() string { return operationKeyField }

// contextKeyOperationSeq counts tool calls within one request.
type contextKeyOperationSeq struct{}

// OperationCounter tracks how many tool calls a request has made.
//
// Held in the context rather than on the Worker because the count must be per
// request, and one Worker interleaves several. A pointer so the value survives
// context derivation: the framework hands callbacks a derived context, and a
// plain int would be copied and lost.
type OperationCounter struct {
	n int
}

// NewOperationCounter returns a counter for one request.
func NewOperationCounter() *OperationCounter { return &OperationCounter{} }

// Next returns the next sequence number, starting at 1.
//
// Not safe for concurrent use, and deliberately not locked: tool calls within
// one agent turn are sequential, and a lock here would suggest otherwise.
// Should the framework ever parallelise them, the sequence would need to come
// from the framework's own call id instead — a lock would hide that need
// rather than address it.
func (c *OperationCounter) Next() int {
	if c == nil {
		return 0
	}
	c.n++
	return c.n
}

// WithOperationCounter attaches a counter to ctx.
func WithOperationCounter(ctx context.Context, c *OperationCounter) context.Context {
	return context.WithValue(ctx, contextKeyOperationSeq{}, c)
}

// OperationCounterFrom reads the counter, or nil when none was attached.
func OperationCounterFrom(ctx context.Context) *OperationCounter {
	c, _ := ctx.Value(contextKeyOperationSeq{}).(*OperationCounter)
	return c
}
