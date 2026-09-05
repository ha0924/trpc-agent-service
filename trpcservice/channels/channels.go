// 设计依据：docs/IM通道接入设计.md §2「Channel 抽象」

// Package channels adapts IM platforms into platform messages, following and
// extending the openclaw Channel model.
//
// This file holds only the registry. Each channel lives in its own
// subpackage, so adding WeCom or Telegram means registering an
// implementation rather than editing the dispatch path.
package channels

import (
	"fmt"
	"sort"
	"sync"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Registry resolves a channel implementation by the name stored in
// channel_bindings.channel.
type Registry struct {
	mu       sync.RWMutex
	inbound  map[string]types.InboundChannel
	outbound map[string]types.OutboundChannel
}

var _ types.Registry = (*Registry)(nil)

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		inbound:  make(map[string]types.InboundChannel),
		outbound: make(map[string]types.OutboundChannel),
	}
}

// Register adds a channel that handles both directions.
//
// Inbound and outbound are stored separately because they are consumed by
// different processes: Gateway only ever needs the inbound half, Worker only
// the outbound half. A channel may implement one without the other.
func (r *Registry) Register(name string, ch types.Channel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inbound[name] = ch
	r.outbound[name] = ch
}

// RegisterInbound adds an inbound-only channel.
func (r *Registry) RegisterInbound(name string, ch types.InboundChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inbound[name] = ch
}

// RegisterOutbound adds an outbound-only channel.
func (r *Registry) RegisterOutbound(name string, ch types.OutboundChannel) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outbound[name] = ch
}

// Inbound returns the inbound half for name.
func (r *Registry) Inbound(name string) (types.InboundChannel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, ok := r.inbound[name]
	if !ok {
		return nil, fmt.Errorf("no inbound channel registered for %q", name)
	}
	return ch, nil
}

// Outbound returns the outbound half for name.
func (r *Registry) Outbound(name string) (types.OutboundChannel, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ch, ok := r.outbound[name]
	if !ok {
		return nil, fmt.Errorf("no outbound channel registered for %q", name)
	}
	return ch, nil
}

// Names lists registered channels, sorted for stable logging.
func (r *Registry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	seen := make(map[string]bool, len(r.inbound)+len(r.outbound))
	for n := range r.inbound {
		seen[n] = true
	}
	for n := range r.outbound {
		seen[n] = true
	}

	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
