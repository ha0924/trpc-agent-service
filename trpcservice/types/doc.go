// Package types defines the platform's core contracts: the unified message
// model, the per-request tenant context, the configuration entities read from
// the control plane, and the four interfaces that every other package depends
// on.
//
// # Why this package exists
//
// types is the only package with no internal dependencies. Everything else may
// import it; it imports nothing from trpcservice. Freezing these contracts
// first is what allows the remaining packages to be built in parallel without
// blocking each other.
//
// # The four core interfaces
//
//	Channel            message intake and delivery, plus capability description
//	SessionDispatcher  publish and consume pending-session hints
//	StorageRouter      pick a backend by tenant + agent + data type
//	RuntimeProvider    assemble a Runtime from tenant + version
//
// These four are fixed in phase one. Later phases add implementations, not
// new shapes.
//
// # Reuse of tRPC-Agent-Go and openclaw
//
// Channel embeds openclaw's channel.Channel rather than redeclaring it, and
// the message model builds on gwproto.ContentPart rather than inventing its
// own media types. Only those two openclaw packages are depended on: they are
// small and stable, whereas openclaw's Gateway lives under internal/ and is
// not importable. See docs/框架复用与扩展.md for the full boundary.
//
// # Traceability
//
// Each file in this package carries a "设计依据" comment naming the design
// document section it implements, so a reviewer can go from code to rationale
// without guessing:
//
//	context.go     docs/技术设计方案.md §3.2 所有请求携带租户上下文
//	message.go     docs/IM通道接入设计.md §3 统一消息模型
//	channel.go     docs/IM通道接入设计.md §2 Channel 抽象
//	dispatcher.go  docs/多租户与节点部署设计.md §5 调度模型
//	storage.go     docs/数据同步与多后端适配.md §1 Storage Router
//	runtime.go     docs/多租户与节点部署设计.md §6 Runtime 装配与缓存
//	config.go      docs/数据模型设计.md §5 核心表结构
package types
