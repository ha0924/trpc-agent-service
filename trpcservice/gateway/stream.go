// 设计依据：docs/IM通道接入设计.md §9.4「单连接约束与 per-bot 选主」、§9.5「Gateway 不再完全无状态」
//                docs/多租户与节点部署设计.md §5.1.1「长连接通道的反向通路」

package gateway

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// StreamSupervisor keeps one connection open per stream-mode binding.
//
// This type is what turns openclaw's Run(ctx) from a method every channel
// implemented and nobody called into a real scheduling point.
//
// Two platform constraints shape it, and both come from the IM side rather
// than from choice:
//
//   - A provider may allow a bot only one live connection, kicking the older
//     one when a new subscription succeeds. Several Gateway replicas
//     connecting freely would displace each other in a loop, so exactly one
//     replica may hold each binding: a connection lease elects it, the rest
//     stay warm standbys.
//   - A reply can only leave through the socket it arrived on. The Worker
//     cannot call the holding replica — the two processes never call each
//     other — so replies come back through a Redis outbox that the holder
//     drains. See senderLoop.
//
// The consequence, recorded rather than hidden: for stream bindings Gateway
// is no longer stateless. Failover is bound by the lease TTL.
type StreamSupervisor struct {
	gw       *Gateway
	channels map[string]types.StreamChannel
	outbox   types.StreamOutbox
	leases   types.ConnectionLease
	owner    string
	log      *slog.Logger

	// retryInterval is how often a standby retries the lease, and how often a
	// failed connection is reattempted.
	retryInterval time.Duration
	// renewInterval must stay well under the lease TTL, or a live holder
	// would lose its lease to a standby while still connected.
	renewInterval time.Duration
	// pollInterval is how often the holder checks the outbox for replies.
	pollInterval time.Duration

	// stateMu guards state, which exists only so the health endpoint can
	// answer "is this replica the holder, and is anything stuck". Without it
	// a dead holder is invisible: every other signal stays green.
	stateMu sync.RWMutex
	state   map[string]*bindingState
}

// bindingState is the supervisor's view of one binding.
type bindingState struct {
	binding *types.ChannelBinding
	held    bool
}

// StreamDeps are the collaborators a StreamSupervisor needs.
type StreamDeps struct {
	// Channels maps channel name to its stream implementation. Only bindings
	// whose channel appears here are supervised; a stream binding naming an
	// unregistered channel is reported once at startup rather than silently
	// ignored, because "configured but never connected" is the failure mode
	// hardest to notice.
	Channels map[string]types.StreamChannel
	Outbox   types.StreamOutbox
	Leases   types.ConnectionLease
	// Owner identifies this replica in lease values, for diagnosing which
	// process holds a given bot.
	Owner  string
	Logger *slog.Logger
}

// NewStreamSupervisor builds a supervisor.
func NewStreamSupervisor(gw *Gateway, d StreamDeps) (*StreamSupervisor, error) {
	switch {
	case gw == nil:
		return nil, errors.New("gateway: stream supervisor requires a gateway")
	case d.Outbox == nil:
		return nil, errors.New("gateway: stream supervisor requires an outbox")
	case d.Leases == nil:
		return nil, errors.New("gateway: stream supervisor requires a connection lease")
	case d.Owner == "":
		return nil, errors.New("gateway: stream supervisor requires an owner id")
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	chans := d.Channels
	if chans == nil {
		chans = map[string]types.StreamChannel{}
	}
	return &StreamSupervisor{
		gw:            gw,
		channels:      chans,
		outbox:        d.Outbox,
		leases:        d.Leases,
		owner:         d.Owner,
		log:           logger,
		retryInterval: 10 * time.Second,
		renewInterval: 15 * time.Second,
		pollInterval:  500 * time.Millisecond,
		state:         make(map[string]*bindingState),
	}, nil
}

// Run supervises every stream binding until ctx is cancelled.
//
// Bindings are read once at startup. Picking up a newly configured binding
// needs a restart, which is honest about what is implemented rather than
// pretending a rescan loop exists; a webhook binding still takes effect
// without one, since those are resolved per request.
func (s *StreamSupervisor) Run(ctx context.Context) error {
	bindings, err := s.gw.store.StreamBindings(ctx)
	if err != nil {
		return err
	}
	if len(bindings) == 0 {
		s.log.Debug("no stream bindings configured")
		return nil
	}

	var wg sync.WaitGroup
	supervised := 0
	for i := range bindings {
		b := bindings[i]
		ch, ok := s.channels[b.Channel]
		if !ok {
			// Loud, because a stream binding with no implementation will
			// never receive a message and nothing else would say so.
			s.log.Error("stream binding has no channel implementation",
				"channel", b.Channel, "channel_binding_id", b.ChannelBindingID,
				"tenant_id", b.TenantID)
			continue
		}
		supervised++
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.supervise(ctx, &b, ch)
		}()
	}

	s.log.Info("stream supervisor started",
		"bindings", supervised, "owner", s.owner)
	wg.Wait()
	return nil
}

// supervise runs the elect-connect-standby cycle for one binding.
func (s *StreamSupervisor) supervise(
	ctx context.Context,
	binding *types.ChannelBinding,
	ch types.StreamChannel,
) {
	log := s.log.With(
		"tenant_id", binding.TenantID,
		"agent_app_id", binding.AgentAppID,
		"channel", binding.Channel,
		"channel_binding_id", binding.ChannelBindingID)

	// Registered before the first election attempt, so a binding that never
	// wins the lease still appears in Health as a standby rather than being
	// absent altogether.
	s.setHeld(binding, false)

	for ctx.Err() == nil {
		won, err := s.leases.AcquireConnection(ctx, binding.ChannelBindingID, s.owner)
		if err != nil {
			// Redis trouble. Back off and retry: connecting without the lease
			// would risk two replicas fighting over the one allowed socket.
			log.Warn("acquire connection lease failed",
				"error", applog.Scrub(err.Error()))
			s.sleep(ctx, s.retryInterval)
			continue
		}
		if !won {
			// Another replica holds it. This is the expected state for all
			// but one replica, so it is not an error and not logged as one.
			log.Debug("another replica holds the connection, standing by")
			s.sleep(ctx, s.retryInterval)
			continue
		}

		log.Info("connection lease acquired, connecting")
		s.setHeld(binding, true)
		s.hold(ctx, binding, ch, log)
		s.setHeld(binding, false)

		// Release before retrying so a standby can take over promptly rather
		// than waiting out the TTL. A fresh context: ctx may already be
		// cancelled, and the release still has to happen.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		if err := s.leases.ReleaseConnection(releaseCtx, binding.ChannelBindingID, s.owner); err != nil {
			log.Warn("release connection lease failed", "error", applog.Scrub(err.Error()))
		}
		cancel()

		if ctx.Err() == nil {
			s.sleep(ctx, s.retryInterval)
		}
	}
}

// hold runs the connection, its lease renewal and its reply sender, and
// returns when any of the three ends.
//
// The three are bound to one context so losing the lease tears down the
// connection: the platform has already given the socket to whoever took the
// lease, and continuing to read from a replaced connection would mean
// processing messages the new holder is also processing.
func (s *StreamSupervisor) hold(
	ctx context.Context,
	binding *types.ChannelBinding,
	ch types.StreamChannel,
	log *slog.Logger,
) {
	holdCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		defer cancel()
		s.renewLoop(holdCtx, binding, log)
	}()

	go func() {
		defer wg.Done()
		defer cancel()
		s.senderLoop(holdCtx, binding, ch, log)
	}()

	sink := s.gw.SinkFor(binding)
	err := ch.Run(holdCtx, binding, sink)
	cancel()
	wg.Wait()

	switch {
	case err == nil, errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		log.Info("stream connection closed")
	default:
		log.Error("stream connection failed", "error", applog.Scrub(err.Error()))
	}
}

// renewLoop keeps the connection lease alive while the connection is up.
func (s *StreamSupervisor) renewLoop(
	ctx context.Context,
	binding *types.ChannelBinding,
	log *slog.Logger,
) {
	ticker := time.NewTicker(s.renewInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ok, err := s.leases.RenewConnection(ctx, binding.ChannelBindingID, s.owner)
			if err != nil {
				log.Warn("renew connection lease failed",
					"error", applog.Scrub(err.Error()))
				// Uncertain state: stop rather than keep a possibly-orphaned
				// connection open. Fail-closed matches how the Worker treats
				// a failed session-lease renewal.
				return
			}
			if !ok {
				log.Warn("connection lease lost, closing connection")
				return
			}
		}
	}
}

// senderLoop drains the binding's outbox and writes replies to the live
// connection.
//
// This is the reverse-path consumer. It runs only in the replica holding the
// connection, which is precisely why the Worker cannot do it: the Worker has
// no socket, and asking it to find the replica that does would require the
// service discovery this architecture does without.
//
// Polling rather than a blocking pop because the poll has to notice lease
// loss and shutdown promptly; the interval is short and the outbox is empty
// in the common case.
func (s *StreamSupervisor) senderLoop(
	ctx context.Context,
	binding *types.ChannelBinding,
	ch types.StreamChannel,
	log *slog.Logger,
) {
	sender, ok := ch.(types.StreamSender)
	if !ok {
		// The channel can receive but not reply. Worth saying once: replies
		// would pile up in the outbox with no other symptom.
		log.Error("stream channel cannot send replies",
			"channel", binding.Channel)
		return
	}

	ticker := time.NewTicker(s.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Drain fully each tick: a burst of replies should not be spread
			// across as many ticks as there are messages.
			for {
				reply, err := s.outbox.PopReply(ctx, binding.ChannelBindingID)
				if err != nil {
					log.Warn("pop reply failed", "error", applog.Scrub(err.Error()))
					break
				}
				if reply == nil {
					break
				}
				s.deliver(ctx, sender, binding, reply, log)
			}
		}
	}
}

// deliver writes one reply out and records the outcome.
func (s *StreamSupervisor) deliver(
	ctx context.Context,
	sender types.StreamSender,
	binding *types.ChannelBinding,
	reply *types.StreamReply,
	log *slog.Logger,
) {
	l := log.With("session_id", reply.SessionID, "request_id", reply.RequestID,
		"trace_id", reply.TraceID)

	sendCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	if err := sender.SendReply(sendCtx, binding, reply); err != nil {
		// The agent has already run. Marking delivery — not execution —
		// failed is what stops the sweeper from replaying tool calls.
		l.Error("stream reply delivery failed", "error", applog.Scrub(err.Error()))
		s.markDeliveryFailed(ctx, reply, err, l)
		return
	}

	markCtx, markCancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer markCancel()
	if err := s.gw.store.UpdateInboundState(markCtx, reply.ChannelBindingID,
		reply.ExternalEventID, types.StateSucceeded, ""); err != nil {
		l.Warn("mark inbound succeeded failed", "error", applog.Scrub(err.Error()))
	}
	l.Info("stream reply delivered")
}

func (s *StreamSupervisor) markDeliveryFailed(
	ctx context.Context,
	reply *types.StreamReply,
	cause error,
	log *slog.Logger,
) {
	markCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.gw.store.UpdateInboundState(markCtx, reply.ChannelBindingID,
		reply.ExternalEventID, types.StateDeliveryFailed,
		applog.Scrub(cause.Error())); err != nil {
		log.Warn("mark delivery failed failed", "error", applog.Scrub(err.Error()))
	}
}

// BindingHealth is one stream binding's observable state.
type BindingHealth struct {
	ChannelBindingID string `json:"channel_binding_id"`
	TenantID         string `json:"tenant_id"`
	Channel          string `json:"channel"`
	// Held reports whether this replica currently holds the connection.
	Held bool `json:"held"`
	// OutboxDepth is how many replies are waiting to go out.
	//
	// This is the field worth alerting on. When the holding replica dies,
	// every other signal stays green — Gateway listens, Worker consumes,
	// MySQL and Redis answer — and the only symptom is replies piling up
	// here. See 风险清单.md #13.
	OutboxDepth int `json:"outbox_depth"`
}

// Health reports each supervised binding's state, for the health endpoint.
func (s *StreamSupervisor) Health(ctx context.Context) []BindingHealth {
	s.stateMu.RLock()
	bindings := make([]*types.ChannelBinding, 0, len(s.state))
	held := make(map[string]bool, len(s.state))
	for id, st := range s.state {
		bindings = append(bindings, st.binding)
		held[id] = st.held
	}
	s.stateMu.RUnlock()

	out := make([]BindingHealth, 0, len(bindings))
	for _, b := range bindings {
		h := BindingHealth{
			ChannelBindingID: b.ChannelBindingID,
			TenantID:         b.TenantID,
			Channel:          b.Channel,
			Held:             held[b.ChannelBindingID],
		}
		if n, err := s.outbox.ReplyLen(ctx, b.ChannelBindingID); err == nil {
			h.OutboxDepth = n
		}
		out = append(out, h)
	}
	return out
}

// setHeld records whether this replica holds a binding's connection.
func (s *StreamSupervisor) setHeld(binding *types.ChannelBinding, held bool) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	st, ok := s.state[binding.ChannelBindingID]
	if !ok {
		st = &bindingState{binding: binding}
		s.state[binding.ChannelBindingID] = st
	}
	st.held = held
}

// sleep waits for d or until ctx ends, whichever comes first.
func (s *StreamSupervisor) sleep(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
