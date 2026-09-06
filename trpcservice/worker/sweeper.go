// 设计依据：docs/风险清单.md #5「毒消息阻塞 Session 信箱」、其他已知约束「消息队列丢消息」

package worker

import (
	"context"
	"log/slog"
	"time"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// SweepConfig tunes the reconciliation loop.
type SweepConfig struct {
	// Interval is how often to look for stranded requests.
	Interval time.Duration
	// Age is how long a request must sit in processing before it is
	// considered stranded. It has to exceed the longest plausible agent run,
	// otherwise the sweep requeues work that is still under way.
	Age time.Duration
	// Batch bounds one pass, so a large backlog is worked through gradually
	// rather than flooding the queue in a single tick.
	Batch int
	// MaxDeliveryAttempts bounds redelivery of one reply. Past the bound the
	// request becomes terminal: an undeliverable reply should stop consuming
	// the platform's outbound rate limit indefinitely.
	MaxDeliveryAttempts int
}

// Sweeper requeues requests that never reached a terminal state.
//
// It is the counterpart to two deliberate design choices, and without it both
// become quiet failures:
//
//   - The queue is allowed to lose hints. A lost hint leaves its
//     inbound_events row in processing and nothing else would notice; writing
//     that row before the ACK is only useful if something reads it back.
//   - A failed message is left in processing rather than retried inline.
//     Without a sweep to requeue it the attempt counter never advances and
//     max_message_attempts never fires, so the dead letter would stay empty
//     no matter how badly a message failed.
//
// It runs in the Worker rather than as a separate process: it needs the same
// mailbox, dispatcher and database, and a third deployable to operate would be
// a poor trade for one loop.
type Sweeper struct {
	cfg       SweepConfig
	store     *store.Store
	mailbox   types.SessionMailbox
	queue     types.SessionDispatcher
	redeliver Redeliverer
	log       *slog.Logger
}

// Redeliverer sends an already-produced reply again.
//
// Separate from the normal execution path on purpose. Once the agent has run,
// a delivery failure must retry *only* the delivery: the tools already ran and
// their side effects already happened, so re-executing would place a second
// order or send a second notification.
//
// The original inbound message is passed through because the delivery state
// machine is keyed by its ExternalEventID, and long-connection channels need
// its CorrelationID to answer the original exchange rather than push a new
// message.
//
// A true queued return means the reply was handed to another process for
// sending, so the caller must not record a terminal state.
type Redeliverer interface {
	Redeliver(ctx context.Context, tenantID, sessionID, requestID, reply string,
		original *types.InboundMessage) (queued bool, err error)
}

// NewSweeper builds a Sweeper, filling defaults.
func NewSweeper(cfg SweepConfig, s *store.Store, mailbox types.SessionMailbox, queue types.SessionDispatcher, logger *slog.Logger) *Sweeper {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = time.Minute
	}
	if cfg.Age <= 0 {
		// Comfortably longer than a slow model call plus tool round trips.
		cfg.Age = 5 * time.Minute
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 50
	}
	if cfg.MaxDeliveryAttempts <= 0 {
		cfg.MaxDeliveryAttempts = 5
	}
	return &Sweeper{cfg: cfg, store: s, mailbox: mailbox, queue: queue, log: logger}
}

// WithRedeliverer enables the failed-delivery pass. Without one the sweeper
// only requeues stranded requests; the delivery_failed rows would accumulate
// with nobody consuming them.
func (s *Sweeper) WithRedeliverer(r Redeliverer) *Sweeper {
	s.redeliver = r
	return s
}

// Run sweeps until ctx is cancelled.
func (s *Sweeper) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()

	s.log.Info("sweeper started",
		"interval", s.cfg.Interval.String(),
		"age", s.cfg.Age.String(),
		"batch", s.cfg.Batch)

	for {
		select {
		case <-ctx.Done():
			s.log.Info("sweeper stopped")
			return
		case <-ticker.C:
			if n := s.sweepOnce(ctx); n > 0 {
				s.log.Info("stranded requests requeued", "count", n)
			}
			if n := s.retryDeliveries(ctx); n > 0 {
				s.log.Info("replies redelivered", "count", n)
			}
		}
	}
}

// sweepOnce requeues one batch and returns how many were requeued.
func (s *Sweeper) sweepOnce(ctx context.Context) int {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	stale, err := s.store.FindStaleInbound(ctx, s.cfg.Age, s.cfg.Batch)
	if err != nil {
		s.log.Error("finding stranded requests failed", "error", applog.Scrub(err.Error()))
		return 0
	}

	requeued := 0
	for _, si := range stale {
		log := s.log.With(
			"tenant_id", si.TenantID,
			"session_id", si.SessionID,
			"request_id", si.RequestID,
			"trace_id", si.TraceID,
			"stranded_for", time.Since(si.UpdatedAt).Round(time.Second).String())

		// updated_at is bumped first. If it were bumped last and the requeue
		// failed halfway, the next tick would pick the same row up again and
		// pile duplicates into the mailbox.
		if err := s.store.TouchInbound(ctx, si.ChannelBindingID, si.ExternalEventID); err != nil {
			log.Error("touching stranded request failed", "error", applog.Scrub(err.Error()))
			continue
		}

		if err := s.mailbox.Push(ctx, si.SessionID, si.Payload); err != nil {
			log.Error("requeueing into mailbox failed", "error", applog.Scrub(err.Error()))
			continue
		}
		if err := s.queue.Publish(ctx, types.SessionHint{
			TenantID:   si.TenantID,
			SessionID:  si.SessionID,
			AgentAppID: si.Payload.AgentAppID,
			TraceID:    si.TraceID,
		}); err != nil {
			// The message is safely in the mailbox; only the wake-up was lost.
			// A later hint for this session, or the next sweep, picks it up.
			log.Error("publishing hint for stranded request failed", "error", applog.Scrub(err.Error()))
			continue
		}

		log.Warn("stranded request requeued", "prior_attempts", si.Attempts)
		requeued++
	}
	return requeued
}

// retryDeliveries resends replies that were produced but never delivered.
//
// The distinction from sweepOnce is the whole point: a stranded request is
// re-executed, a failed delivery is only re-sent. Collapsing the two would
// repeat every tool call for a message whose only problem was a network
// hiccup on the way out.
func (s *Sweeper) retryDeliveries(ctx context.Context) int {
	if s.redeliver == nil {
		return 0
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	failed, err := s.store.FindFailedDeliveries(ctx, s.cfg.Age, s.cfg.MaxDeliveryAttempts, s.cfg.Batch)
	if err != nil {
		s.log.Error("finding failed deliveries failed", "error", applog.Scrub(err.Error()))
		return 0
	}

	sent := 0
	for _, si := range failed {
		log := s.log.With(
			"tenant_id", si.TenantID,
			"session_id", si.SessionID,
			"request_id", si.RequestID,
			"trace_id", si.TraceID,
			"attempts", si.Attempts)

		// The reply is read back from the session rather than kept on the
		// inbound row, so there is one copy of the answer.
		reply, err := s.store.LastAgentReply(ctx, si.TenantID, si.SessionID, si.RequestID)
		if err != nil {
			// No stored reply means the agent never actually finished, so
			// this row was mislabelled. Mark it failed rather than leaving it
			// to be retried forever against a reply that does not exist.
			log.Error("no stored reply to redeliver, marking failed",
				"error", applog.Scrub(err.Error()))
			if err := s.store.UpdateInboundState(ctx, si.ChannelBindingID, si.ExternalEventID,
				types.StateFailed, "delivery_failed with no stored reply"); err != nil {
				log.Error("marking failed failed", "error", applog.Scrub(err.Error()))
			}
			continue
		}

		queued, err := s.redeliver.Redeliver(ctx, si.TenantID, si.SessionID, si.RequestID,
			reply, si.Payload)
		if err != nil {
			// UpdateInboundState increments attempts, so repeated failures
			// walk toward MaxDeliveryAttempts and the row eventually drops
			// out of the query above.
			log.Warn("redelivery failed", "error", applog.Scrub(err.Error()))
			if err := s.store.UpdateInboundState(ctx, si.ChannelBindingID, si.ExternalEventID,
				types.StateDeliveryFailed, applog.Scrub(err.Error())); err != nil {
				log.Error("recording redelivery failure failed", "error", applog.Scrub(err.Error()))
			}
			continue
		}

		// A queued reply is not a delivered one: for a stream binding the
		// send happens in whichever Gateway replica holds the connection,
		// and only that replica learns the outcome. Marking succeeded here
		// would report a delivery that may still fail at the socket, with no
		// row left to retry.
		//
		// The row stays in delivery_failed and the holder writes the
		// terminal state. attempts was already incremented, so this cannot
		// loop forever — it walks toward MaxDeliveryAttempts like any other
		// stuck row.
		if queued {
			log.Info("reply queued for stream redelivery; holder will record the outcome")
			sent++
			continue
		}

		if err := s.store.UpdateInboundState(ctx, si.ChannelBindingID, si.ExternalEventID,
			types.StateSucceeded, ""); err != nil {
			log.Error("marking succeeded failed", "error", applog.Scrub(err.Error()))
		}
		log.Info("reply redelivered without re-running the agent")
		sent++
	}
	return sent
}
