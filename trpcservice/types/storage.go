// 设计依据：docs/数据同步与多后端适配.md §1「Storage Adapter / Router」
//                docs/框架复用与扩展.md §3.3「Storage Router」

package types

import (
	"context"

	"trpc.group/trpc-go/trpc-agent-go/session"
)

// DataType names a category of data that may live on its own backend. The
// routing key is always the triple tenant + agent + data type, because two
// tenants may want the same data on different stores and one tenant may want
// sessions on Redis while knowledge sits in a vector database.
type DataType string

const (
	DataTypeSession   DataType = "session"
	DataTypeMemory    DataType = "memory"
	DataTypeSummary   DataType = "summary"
	DataTypeKnowledge DataType = "knowledge"
	DataTypeArtifact  DataType = "artifact"
	DataTypeAudit     DataType = "audit"
)

// BackendRef identifies a configured backend instance. In phase one the
// mapping is read from a config file; from phase two it comes from
// backend_configs and backend_bindings. The reference shape does not change
// when the source does, which is why the router can be built before those
// tables exist.
type BackendRef struct {
	// Name is the backend instance identifier, e.g. "redis-main".
	Name string `json:"name"`
	// Kind is the backend technology, e.g. "redis", "mysql", "s3", "qdrant".
	Kind string `json:"kind"`
	// DSNRef is a secret reference, never a plaintext connection string.
	DSNRef string `json:"dsn_ref,omitempty"`
}

// StorageRouter selects the backend serving a given tenant, agent and data
// type.
//
// It is also a session.Service. That is the important part: the framework's
// runner takes a session.Service through runner.WithSessionService, so making
// the router implement that interface puts per-tenant routing directly on the
// framework's execution path instead of bolting a second data-access layer
// beside it. A call arriving at the router carries its tenant in the context,
// the router picks the backing service, and delegates.
//
// The cost is implementing every session.Service method as a delegation. The
// benefit is that shared session state, cross-node visibility and stateless
// Workers all fall out of one mechanism.
type StorageRouter interface {
	session.Service

	// Resolve reports which backend serves this triple. Used for diagnostics
	// and by non-session data types that do not go through session.Service.
	Resolve(ctx context.Context, tenantID, agentAppID string, dt DataType) (BackendRef, error)

	// SessionService returns the concrete service backing sessions for this
	// tenant and agent. Exposed so callers that already know their tenant can
	// skip the context lookup.
	SessionService(ctx context.Context, tenantID, agentAppID string) (session.Service, error)

	// Close releases all backend connections the router owns.
	Close() error
}
