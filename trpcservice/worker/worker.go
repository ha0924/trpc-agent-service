// 设计依据：docs/多租户与节点部署设计.md §5「调度模型」、§7「水平扩缩容」
//                docs/IM通道接入设计.md §8「出站回复」

// Package worker consumes queued sessions and runs agents.
//
// One round of work looks like this:
//
//	take a hint → win the session lease → drain the mailbox → release
//
// The lease is held per round rather than for the conversation's lifetime, so
// the next round may land on any healthy Worker. That is what makes Workers
// interchangeable and sticky sessions unnecessary. Its TTL doubles as the
// failure-recovery bound: a Worker that dies mid-round blocks its session only
// until the lease expires.
//
// Losing a race for the lease is the normal case, not an error. With
// at-least-once delivery several Workers routinely receive the same hint; all
// but one simply move on.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"trpc.group/trpc-go/trpc-agent-go/event"
	"trpc.group/trpc-go/trpc-agent-go/model"

	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Deps are the collaborators a Worker needs.
type Deps struct {
	Config     *config.Config
	Store      *store.Store
	Dispatcher types.SessionDispatcher
	Mailbox    types.SessionMailbox
	Lease      types.SessionLease
	Runtimes   types.RuntimeProvider
	Channels   *channels.Registry
	Metrics    *metrics.Recorder
	Usage      UsageSink
	Audit      types.AuditSink
	Tracer     *telemetry.Provider
	DeadLetter DeadLetterSink
	RateLimit  RateLimiter
	// Outbox carries replies for stream-mode bindings back to the Gateway
	// replica holding the connection. Optional: a deployment with no
	// long-connection bindings needs none, and a stream binding without one
	// fails loudly at delivery rather than quietly dropping replies.
	Outbox types.StreamOutbox
	Logger *slog.Logger
}

// RateLimiter caps outbound messages per binding.
//
// Triggering the limit queues rather than drops: the agent has already run,
// so discarding the reply means the user gets nothing for work that was
// already paid for.
type RateLimiter interface {
	AllowRate(ctx context.Context, scope string, limitPerMin int) (bool, error)
}

// DeadLetterSink bounds how often one message may be retried and holds the
// ones that exhaust their budget.
//
// Without it a message that always fails is retried forever and every later
// message in that conversation waits behind it — the session is dead while
// looking merely slow.
type DeadLetterSink interface {
	// RecordAttempt increments and returns the try count for a message.
	RecordAttempt(ctx context.Context, requestID string) (int, error)
	// ClearAttempts drops the counter once a message succeeds.
	ClearAttempts(ctx context.Context, requestID string) error
	// PushDeadLetter parks a message that exhausted its budget.
	PushDeadLetter(ctx context.Context, sessionID string, dl *scheduler.DeadLetter) error
}

// UsageSink persists token and cost detail for reconciliation. Separate from
// the budget counter: the counter enforces, this records.
type UsageSink interface {
	WriteUsage(ctx context.Context, u *types.UsageRecord) error
}

// Worker consumes session hints and executes agents.
type Worker struct {
	cfg        *config.Config
	store      *store.Store
	dispatcher types.SessionDispatcher
	mailbox    types.SessionMailbox
	lease      types.SessionLease
	runtimes   types.RuntimeProvider
	channels   *channels.Registry
	metrics    *metrics.Recorder
	usage      UsageSink
	audit      types.AuditSink
	tracer     *telemetry.Provider
	deadLetter DeadLetterSink
	rateLimit  RateLimiter
	outbox     types.StreamOutbox
	log        *slog.Logger

	// id identifies this process as a lease owner. Two Workers on one host
	// must differ, or each would be able to release the other's lease.
	id string

	// slots bounds concurrent sessions. Model calls are slow and the limit
	// here, not queue depth, is what actually caps load on the provider.
	slots chan struct{}

	wg sync.WaitGroup
}

// New builds a Worker.
func New(d Deps) (*Worker, error) {
	switch {
	case d.Config == nil:
		return nil, errors.New("worker: config is required")
	case d.Store == nil:
		return nil, errors.New("worker: store is required")
	case d.Dispatcher == nil:
		return nil, errors.New("worker: dispatcher is required")
	case d.Mailbox == nil:
		return nil, errors.New("worker: mailbox is required")
	case d.Lease == nil:
		return nil, errors.New("worker: lease is required")
	case d.Runtimes == nil:
		return nil, errors.New("worker: runtime provider is required")
	case d.Channels == nil:
		return nil, errors.New("worker: channel registry is required")
	}

	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}

	id := strings.TrimSpace(d.Config.Worker.ID)
	if id == "" {
		host, err := os.Hostname()
		if err != nil {
			host = "unknown"
		}
		id = fmt.Sprintf("%s-%d", host, os.Getpid())
	}

	rec := d.Metrics
	if rec == nil {
		// Instrumentation must never be the reason a message fails.
		rec = metrics.NewRecorder(metrics.NewRegistry())
	}

	return &Worker{
		cfg: d.Config, store: d.Store, dispatcher: d.Dispatcher,
		mailbox: d.Mailbox, lease: d.Lease, runtimes: d.Runtimes,
		channels: d.Channels, metrics: rec, usage: d.Usage, audit: d.Audit,
		tracer: d.Tracer, deadLetter: d.DeadLetter, rateLimit: d.RateLimit,
		outbox: d.Outbox,
		log:    logger, id: id,
		slots: make(chan struct{}, d.Config.Worker.Concurrency),
	}, nil
}

// ID reports this Worker's lease owner identity.
func (w *Worker) ID() string { return w.id }

// Run consumes hints until ctx is cancelled, then waits for in-flight rounds.
func (w *Worker) Run(ctx context.Context) error {
	hints, err := w.dispatcher.Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	w.log.Info("worker consuming", "worker_id", w.id, "concurrency", cap(w.slots))

	for {
		select {
		case <-ctx.Done():
			w.drain()
			return nil

		case hint, ok := <-hints:
			if !ok {
				w.drain()
				return nil
			}

			// Acquire a slot before starting, so an oversized burst waits in
			// the queue rather than spawning unbounded goroutines.
			select {
			case w.slots <- struct{}{}:
			case <-ctx.Done():
				w.drain()
				return nil
			}

			w.wg.Add(1)
			go func(h types.SessionHint) {
				defer w.wg.Done()
				defer func() { <-w.slots }()
				w.handleHint(ctx, h)
			}(hint)
		}
	}
}

// drain waits for in-flight rounds to finish.
//
// Rounds are allowed to complete rather than being cut off: a round
// interrupted between running the agent and delivering the reply would leave
// an inbound_events row that must not be re-executed, since its tools have
// already had their effect.
func (w *Worker) drain() {
	done := make(chan struct{})
	go func() { w.wg.Wait(); close(done) }()

	select {
	case <-done:
		w.log.Info("all rounds finished")
	case <-time.After(w.cfg.Worker.ShutdownTimeout):
		w.log.Warn("shutdown timeout reached with rounds still running")
	}
}

// handleHint runs one round for one session.
func (w *Worker) handleHint(ctx context.Context, hint types.SessionHint) {
	// Continue the trace the Gateway started. The hint carries the full W3C
	// context, not just the id, so this span becomes a child of the inbound
	// span rather than the root of a second tree.
	ctx = telemetry.Extract(ctx, hint.TraceContext)
	ctx, span := w.tracer.StartSpan(ctx, "worker.round",
		attribute.String("tenant.id", hint.TenantID),
		attribute.String("agent.app_id", hint.AgentAppID),
		attribute.String("session.id", hint.SessionID),
		attribute.String("worker.id", w.id))
	defer span.End()

	log := w.log.With(
		"worker_id", w.id,
		"tenant_id", hint.TenantID,
		"agent_app_id", hint.AgentAppID,
		"session_id", hint.SessionID,
		"trace_id", hint.TraceID)

	won, err := w.lease.Acquire(ctx, hint.SessionID, w.id)
	if err != nil {
		telemetry.RecordError(span, err)
		log.Error("acquire lease failed", "error", applog.Scrub(err.Error()))
		return
	}
	span.SetAttributes(attribute.Bool("lease.won", won))
	if !won {
		// Expected under at-least-once delivery: another Worker owns this
		// session right now and will drain it, including our message.
		// Recorded as an attribute rather than an error — a lost race is
		// the design working, and marking it an error would make healthy
		// traces look broken.
		span.SetAttributes(attribute.Bool("lease.won", false))
		w.metrics.LeaseContention(hint.TenantID)
		log.Debug("lease held elsewhere, skipping")
		return
	}

	// A round runs under its own context so lease loss can cancel the model
	// call immediately rather than after it returns.
	roundCtx, cancelRound := context.WithCancel(ctx)
	defer cancelRound()

	stopRenew := w.keepLeaseAlive(roundCtx, log, hint.SessionID, cancelRound)
	defer func() {
		stopRenew()
		// Release with a fresh context: the round's context may already be
		// cancelled, and failing to release would idle the session until the
		// TTL expires.
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if err := w.lease.Release(releaseCtx, hint.SessionID, w.id); err != nil {
			log.Warn("release lease failed", "error", applog.Scrub(err.Error()))
		}
	}()

	w.drainMailbox(roundCtx, log, hint)
}

// keepLeaseAlive renews the lease until the round ends.
//
// A failed renewal means the lease was lost and another Worker has taken the
// session. The round is cancelled at once rather than allowed to finish: two
// Workers writing the same conversation is precisely what the lease exists to
// prevent, and the database's sequence uniqueness would turn it into a failed
// write anyway.
func (w *Worker) keepLeaseAlive(ctx context.Context, log *slog.Logger, sessionID string, onLost func()) func() {
	ticker := time.NewTicker(w.cfg.Scheduler.LeaseRenewInterval)
	done := make(chan struct{})

	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				ok, err := w.lease.Renew(ctx, sessionID, w.id)
				if err != nil {
					log.Error("renew lease failed", "error", applog.Scrub(err.Error()))
					onLost()
					return
				}
				if !ok {
					log.Warn("lease lost, aborting round")
					onLost()
					return
				}
			}
		}
	}()

	return func() { close(done) }
}

// drainMailbox processes messages until the mailbox is empty.
func (w *Worker) drainMailbox(ctx context.Context, log *slog.Logger, hint types.SessionHint) {
	for {
		if ctx.Err() != nil {
			return
		}

		msg, err := w.mailbox.Pop(ctx, hint.SessionID)
		if err != nil {
			log.Error("mailbox pop failed", "error", applog.Scrub(err.Error()))
			return
		}
		if msg == nil {
			return // empty: the round is done
		}

		if err := w.processMessage(ctx, log, hint, msg); err != nil {
			log.Error("message processing failed",
				"request_id", msg.RequestID, "error", applog.Scrub(err.Error()))
			w.recordFailure(ctx, log, hint.SessionID, msg, err)
			// Keep draining. A single failing message must not block every
			// later message in this conversation.
			continue
		}
	}
}

// processMessage runs the agent for one message and delivers the reply.
func (w *Worker) processMessage(
	ctx context.Context,
	log *slog.Logger,
	hint types.SessionHint,
	msg *types.InboundMessage,
) error {
	ctx, span := w.tracer.StartSpan(ctx, "worker.message",
		attribute.String("request.id", msg.RequestID),
		attribute.String("channel", msg.Channel))
	defer span.End()

	sess, err := w.store.SessionByID(ctx, msg.TenantID, hint.SessionID)
	if err != nil {
		telemetry.RecordError(span, err)
		return fmt.Errorf("load session: %w", err)
	}

	// The full identity, rebuilt on the Worker side and attached to the
	// context so it reaches the model, every tool and every storage call.
	rc := &types.RequestContext{
		TenantID:         sess.TenantID,
		AgentAppID:       sess.AgentAppID,
		AgentVersion:     sess.AgentVersion,
		Channel:          sess.Channel,
		ChannelBindingID: sess.ChannelBindingID,
		UserID:           msg.ExternalUserID,
		SessionID:        sess.SessionID,
		RequestID:        msg.RequestID,
		TraceID:          msg.TraceID,
	}
	ctx = types.NewContext(ctx, rc)
	log = applog.With(w.log, rc).With("worker_id", w.id)

	// Persist the user's turn before running, so the conversation is on record
	// even if execution fails.
	//
	// Scrubbed unconditionally, not by policy. The design forbids credentials
	// from reaching Session and Memory at all, and session_events is worse
	// than a log: it is long-retained and replayed as conversation history
	// into every later model call, so one pasted credential would be resent
	// on every subsequent turn.
	if _, err := w.store.AppendSessionEvent(ctx, &types.SessionEvent{
		TenantID: rc.TenantID, SessionID: rc.SessionID,
		EventType: types.EventTypeUserMessage, Role: "user",
		Content:   map[string]any{"text": applog.Scrub(msg.Text)},
		RequestID: rc.RequestID, TraceID: rc.TraceID, AgentVersion: rc.AgentVersion,
	}); err != nil {
		return fmt.Errorf("persist user event: %w", err)
	}

	runtime, err := w.runtimes.Get(ctx, rc.RuntimeKey())
	if err != nil {
		return fmt.Errorf("get runtime: %w", err)
	}

	started := time.Now()
	events, err := runtime.Runner.Run(ctx, rc.UserID, rc.SessionID, msg.ToModelMessage())
	if err != nil {
		return fmt.Errorf("run agent: %w", err)
	}

	reply, usage, runErr := w.consumeEvents(ctx, log, rc, events)
	w.metrics.AgentRun(ctx, started, runErr)
	if runErr != nil {
		telemetry.RecordError(span, runErr)
	}
	if usage != nil {
		span.SetAttributes(
			attribute.Int("model.prompt_tokens", usage.PromptTokens),
			attribute.Int("model.completion_tokens", usage.CompletionTokens))
	}

	// Audit every run, not only refusals. The trail has to answer why the
	// platform allowed something as well as why it refused: a record that
	// exists only on denial cannot show that a tenant did use an agent at a
	// given time, which is the question a compliance review actually asks.
	w.auditRun(ctx, rc, runtime, started, usage, runErr)

	if runErr != nil {
		return runErr
	}
	w.recordUsage(ctx, log, rc, runtime, usage, time.Since(started))
	log.Info("agent run finished", "latency_ms", time.Since(started).Milliseconds(),
		"reply_chars", len([]rune(reply)))

	if _, err := w.store.AppendSessionEvent(ctx, &types.SessionEvent{
		TenantID: rc.TenantID, SessionID: rc.SessionID,
		EventType: types.EventTypeAgentMessage, Role: "assistant",
		Content:   map[string]any{"text": applog.Scrub(reply)},
		RequestID: rc.RequestID, TraceID: rc.TraceID, AgentVersion: rc.AgentVersion,
	}); err != nil {
		return fmt.Errorf("persist agent event: %w", err)
	}

	// Delivery is a separate phase from execution. Once the agent has run,
	// a delivery failure must retry only the delivery — rerunning would
	// repeat every tool call.
	deliverCtx, deliverSpan := w.tracer.StartSpan(ctx, "worker.deliver",
		attribute.String("channel", sess.Channel))
	deliverErr := w.deliver(deliverCtx, log, sess, msg, reply)
	if deliverErr != nil {
		telemetry.RecordError(deliverSpan, deliverErr)
	}
	deliverSpan.End()
	w.metrics.Delivery(ctx, sess.Channel, deliverErr)
	if err := deliverErr; err != nil {
		if updErr := w.store.UpdateInboundState(ctx, msg.ChannelBindingID, msg.ExternalEventID,
			types.StateDeliveryFailed, applog.Scrub(err.Error())); updErr != nil {
			log.Error("marking delivery_failed failed", "error", applog.Scrub(updErr.Error()))
		}
		return fmt.Errorf("deliver reply: %w", err)
	}

	if err := w.store.UpdateInboundState(ctx, msg.ChannelBindingID, msg.ExternalEventID,
		types.StateSucceeded, ""); err != nil {
		log.Error("marking succeeded failed", "error", applog.Scrub(err.Error()))
	}
	// Drop the retry counter, so a conversation that recovers does not carry
	// a stale budget into its next failure.
	if w.deadLetter != nil {
		if err := w.deadLetter.ClearAttempts(ctx, msg.RequestID); err != nil {
			log.Warn("clearing attempt counter failed", "error", applog.Scrub(err.Error()))
		}
	}
	return nil
}

// consumeEvents reads the runner's event stream to completion.
//
// Events are taken one at a time and handed to the accumulator rather than
// collected and processed at the end. The channel must also be drained fully
// even when we stop caring about the result: an abandoned channel leaks the
// goroutine feeding it.
//
// Phase one buffers and sends once. Replacing the accumulator with a
// segmented sender is the only change streaming would need.
func (w *Worker) consumeEvents(
	ctx context.Context,
	log *slog.Logger,
	rc *types.RequestContext,
	events <-chan *event.Event,
) (string, *model.Usage, error) {
	var (
		reply    strings.Builder
		runError error
		toolCall int
		usage    *model.Usage
	)

	for e := range events {
		if e == nil || e.Response == nil {
			continue
		}
		if e.Response.Error != nil {
			runError = fmt.Errorf("model error: %s", e.Response.Error.Message)
			continue // keep draining
		}
		// Usage arrives on the settled response, not on partial chunks.
		if e.Response.Usage != nil {
			usage = e.Response.Usage
		}

		for _, choice := range e.Response.Choices {
			if len(choice.Message.ToolCalls) > 0 {
				toolCall += len(choice.Message.ToolCalls)
				for _, tc := range choice.Message.ToolCalls {
					log.Info("tool called", "tool", tc.Function.Name)
					// A tool with side effects would need its audit record
					// written here, before the effect, not after.
					if _, err := w.store.AppendSessionEvent(ctx, &types.SessionEvent{
						TenantID: rc.TenantID, SessionID: rc.SessionID,
						EventType: types.EventTypeToolCall, Role: "assistant",
						Content:   map[string]any{"tool": tc.Function.Name},
						RequestID: rc.RequestID, TraceID: rc.TraceID, AgentVersion: rc.AgentVersion,
					}); err != nil {
						log.Error("persist tool call failed", "error", applog.Scrub(err.Error()))
					}
				}
			}
			// Only the assistant's own words go to the user.
			//
			// A tool result arrives as a message too, with role "tool" and a
			// body of raw JSON meant for the model. Accumulating every
			// non-partial message splices that JSON into the reply — the user
			// sees {"result":7006652} followed by the real answer.
			//
			// Found by running a real model: the stub never calls tools, so
			// this path was not exercised until DeepSeek was wired in.
			//
			// Partial chunks are skipped for a separate reason: a streamed
			// answer would otherwise be concatenated twice.
			if !e.Response.IsPartial &&
				choice.Message.Role == model.RoleAssistant &&
				choice.Message.Content != "" {
				reply.WriteString(choice.Message.Content)
			}
		}
	}

	if runError != nil {
		return "", usage, runError
	}
	if reply.Len() == 0 {
		return "", usage, errors.New("agent produced no reply")
	}
	if toolCall > 0 {
		log.Info("tools used in this turn", "count", toolCall)
	}
	return reply.String(), usage, nil
}

// auditRun records one agent execution in the audit trail.
//
// Written through the async sink: an audit write must not add database
// latency to every reply. Records that must be durable before an effect —
// dangerous tool intent — bypass the sink and are written by the guardrail
// itself.
func (w *Worker) auditRun(
	ctx context.Context,
	rc *types.RequestContext,
	runtime *types.Runtime,
	started time.Time,
	usage *model.Usage,
	runErr error,
) {
	if w.audit == nil {
		return
	}

	r := types.NewAuditRecord(rc, types.AuditAgentRun)
	r.LatencyMS = time.Since(started).Milliseconds()
	if runtime != nil {
		r.AgentName = runtime.Key.String()
		if runtime.Spec != nil && usage != nil {
			r.CostUSD = estimateCost(runtime.Spec.ModelName, usage.PromptTokens, usage.CompletionTokens)
		}
	}
	if runErr != nil {
		r.Decision = types.DecisionError
		r.ErrorType = errorType(runErr)
		r.Reason = applog.Scrub(runErr.Error())
	}
	if usage != nil {
		r.Detail = map[string]any{
			"prompt_tokens":     usage.PromptTokens,
			"completion_tokens": usage.CompletionTokens,
			"total_tokens":      usage.TotalTokens,
		}
	}
	if err := w.audit.Write(ctx, r); err != nil {
		w.log.Error("audit write failed", "error", applog.Scrub(err.Error()))
	}
}

// errorType classifies a failure coarsely, so an alert can group by cause
// without parsing free-form messages.
func errorType(err error) string {
	msg := err.Error()
	switch {
	case strings.Contains(msg, "context deadline exceeded"):
		return "timeout"
	case strings.Contains(msg, "context canceled"):
		return "cancelled"
	case strings.Contains(msg, "model error"):
		return "model"
	case strings.Contains(msg, "no reply"):
		return "empty_reply"
	default:
		return "internal"
	}
}

// recordUsage writes token and cost detail and feeds the token metric.
//
// The ledger write is best-effort: losing an accounting row must not fail a
// reply the user has already been promised. Enforcement does not depend on it
// either — that reads the Redis counter, which the budget guardrail updates
// on its own.
func (w *Worker) recordUsage(
	ctx context.Context,
	log *slog.Logger,
	rc *types.RequestContext,
	runtime *types.Runtime,
	usage *model.Usage,
	elapsed time.Duration,
) {
	if usage == nil {
		return
	}
	modelName := ""
	if runtime != nil && runtime.Spec != nil {
		modelName = runtime.Spec.ModelName
	}

	cost := estimateCost(modelName, usage.PromptTokens, usage.CompletionTokens)
	w.metrics.Tokens(ctx, modelName, usage.PromptTokens, usage.CompletionTokens, cost)

	if w.usage == nil {
		return
	}
	if err := w.usage.WriteUsage(ctx, &types.UsageRecord{
		TenantID: rc.TenantID, AgentAppID: rc.AgentAppID, AgentVersion: rc.AgentVersion,
		SessionID: rc.SessionID, RequestID: rc.RequestID, TraceID: rc.TraceID,
		ModelName:        modelName,
		PromptTokens:     usage.PromptTokens,
		CompletionTokens: usage.CompletionTokens,
		TotalTokens:      usage.TotalTokens,
		CostUSD:          cost,
		LatencyMS:        elapsed.Milliseconds(),
	}); err != nil {
		log.Error("usage record write failed", "error", applog.Scrub(err.Error()))
	}
}

// pricePer1KTokens is the per-model rate used to turn tokens into money.
//
// Cost is computed and stored at record time rather than derived on read, so a
// later price change does not silently rewrite historical spend. An unknown
// model yields zero rather than a guess: a wrong number in a billing figure is
// worse than an absent one.
var pricePer1KTokens = map[string]struct{ prompt, completion float64 }{
	"deepseek-chat":     {0.00014, 0.00028},
	"deepseek-reasoner": {0.00055, 0.00219},
	"gpt-4o":            {0.0025, 0.01},
	"gpt-4o-mini":       {0.00015, 0.0006},
}

func estimateCost(model string, prompt, completion int) float64 {
	p, ok := pricePer1KTokens[model]
	if !ok {
		return 0
	}
	return float64(prompt)/1000*p.prompt + float64(completion)/1000*p.completion
}

// deliver sends the reply back through the originating channel.
func (w *Worker) deliver(
	ctx context.Context,
	log *slog.Logger,
	sess *types.Session,
	msg *types.InboundMessage,
	reply string,
) error {
	binding, err := w.store.ChannelBindingByID(ctx, sess.TenantID, sess.ChannelBindingID)
	if err != nil {
		return fmt.Errorf("load binding %s: %w", sess.ChannelBindingID, err)
	}

	// Long replies are split at the channel's limit rather than truncated.
	// Splitting lives here, not in each channel, so every channel behaves the
	// same way.
	parts := splitText(reply, binding.Capabilities.MaxTextLength)

	// A stream binding's reply can only leave through the socket it arrived
	// on, which this process does not hold. It goes to the outbox instead,
	// and the Gateway replica holding the connection sends it.
	//
	// The branch is on capabilities rather than on the channel name, so a
	// second long-connection platform needs no change here.
	if binding.Capabilities.StreamCapable() {
		return w.deliverToOutbox(ctx, log, sess, msg, binding, parts)
	}

	out, err := w.channels.Outbound(sess.Channel)
	if err != nil {
		return err
	}

	for _, part := range parts {
		// Rate limit per binding, checked per part: splitting a long reply
		// into five messages consumes five of the platform's allowance, and
		// exceeding it gets the whole binding throttled.
		if err := w.awaitRateLimit(ctx, log, binding); err != nil {
			return err
		}
		if err := out.Send(ctx, deliveryTarget(sess, msg), types.NewTextReply(part), binding); err != nil {
			return err
		}
	}
	log.Info("reply delivered", "channel", sess.Channel)
	return nil
}

// deliverToOutbox queues a reply for the process holding the connection.
//
// Rate limiting stays on this side even though the send happens elsewhere:
// the limit is the platform's per-conversation allowance, and the Worker is
// where a reply's parts are known. Doing it here also keeps back-pressure
// where the work is, rather than letting the outbox absorb an unbounded
// burst.
//
// Returning an error leaves the message in delivery_failed for the sweeper,
// exactly as a failed HTTP send would — the reverse path deliberately reuses
// the forward path's failure handling rather than inventing its own.
func (w *Worker) deliverToOutbox(
	ctx context.Context,
	log *slog.Logger,
	sess *types.Session,
	msg *types.InboundMessage,
	binding *types.ChannelBinding,
	parts []string,
) error {
	if w.outbox == nil {
		// Configuration gap rather than a runtime fault: a stream binding
		// exists but the Worker was built without an outbox, so no reply
		// could ever reach the user. Say so plainly.
		return fmt.Errorf("stream binding %s requires an outbox, none configured",
			binding.ChannelBindingID)
	}

	for _, part := range parts {
		if err := w.awaitRateLimit(ctx, log, binding); err != nil {
			return err
		}
		reply := &types.StreamReply{
			Channel:          sess.Channel,
			ChannelBindingID: sess.ChannelBindingID,
			TenantID:         sess.TenantID,
			Target:           deliveryTarget(sess, msg),
			Scope:            sess.Scope,
			CorrelationID:    msg.CorrelationID,
			ExternalEventID:  msg.ExternalEventID,
			Text:             part,
			SessionID:        sess.SessionID,
			RequestID:        msg.RequestID,
			TraceID:          msg.TraceID,
			// The W3C context, so the holder's send span joins this trace
			// instead of starting a second tree — the same reason the forward
			// hint carries it.
			TraceContext: telemetry.Inject(ctx),
		}
		if err := w.outbox.PushReply(ctx, reply); err != nil {
			return fmt.Errorf("queue stream reply: %w", err)
		}
	}

	log.Info("reply queued for stream delivery",
		"channel", sess.Channel, "parts", len(parts))
	return nil
}

// deliveryTarget is where the reply goes: the group for a group session, the
// user for a direct one.
func deliveryTarget(sess *types.Session, msg *types.InboundMessage) string {
	if sess.Scope == types.ScopeGroup {
		return sess.ScopeKey
	}
	if msg.ExternalUserID != "" {
		return msg.ExternalUserID
	}
	return sess.ScopeKey
}

// splitText breaks a reply into pieces no longer than limit runes, preferring
// paragraph and sentence boundaries so a split does not land mid-word.
func splitText(s string, limit int) []string {
	runes := []rune(s)
	if limit <= 0 || len(runes) <= limit {
		return []string{s}
	}

	var out []string
	for len(runes) > limit {
		cut := limit
		for _, sep := range []rune{'\n', '。', '.', ' '} {
			for i := limit - 1; i > limit/2; i-- {
				if runes[i] == sep {
					cut = i + 1
					break
				}
			}
			if cut != limit {
				break
			}
		}
		out = append(out, string(runes[:cut]))
		runes = runes[cut:]
	}
	if len(runes) > 0 {
		out = append(out, string(runes))
	}
	return out
}

// recordFailure handles a message that could not be processed.
//
// Bounding attempts is what keeps one poison message from blocking a
// conversation forever. Below the bound the message is left for a later
// attempt; at the bound it is parked in the dead letter and the drain
// continues, so later messages in the same session are served.
//
// Note what is *not* done here: the message is never silently discarded. A
// dropped message looks identical to a message that was never sent, and there
// is then nothing to diagnose.
func (w *Worker) recordFailure(ctx context.Context, log *slog.Logger, sessionID string, msg *types.InboundMessage, cause error) {
	// A fresh context: the round's may already be cancelled, and losing the
	// failure record is worse than the failure.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()

	reason := applog.Scrub(cause.Error())

	if w.deadLetter == nil {
		// No sink configured: fall back to marking terminal, which is the
		// pre-dead-letter behaviour.
		if err := w.store.UpdateInboundState(ctx, msg.ChannelBindingID, msg.ExternalEventID,
			types.StateFailed, reason); err != nil {
			log.Error("recording failure failed", "error", applog.Scrub(err.Error()))
		}
		return
	}

	attempts, err := w.deadLetter.RecordAttempt(ctx, msg.RequestID)
	if err != nil {
		log.Error("counting attempt failed", "error", applog.Scrub(err.Error()))
		attempts = w.cfg.Scheduler.MaxMessageAttempts // fail closed: park it
	}

	if attempts < w.cfg.Scheduler.MaxMessageAttempts {
		log.Warn("message failed, will be retried",
			"attempt", attempts, "max", w.cfg.Scheduler.MaxMessageAttempts, "reason", reason)
		// Left in processing: the sweep of stale rows picks it up, so a
		// transient fault does not need a retry loop here.
		return
	}

	if err := w.deadLetter.PushDeadLetter(ctx, sessionID, &scheduler.DeadLetter{
		Message:   msg,
		Attempts:  attempts,
		LastError: reason,
		FailedAt:  time.Now(),
		WorkerID:  w.id,
	}); err != nil {
		log.Error("pushing dead letter failed", "error", applog.Scrub(err.Error()))
	}

	if err := w.store.UpdateInboundState(ctx, msg.ChannelBindingID, msg.ExternalEventID,
		types.StateFailed, reason); err != nil {
		log.Error("recording failure failed", "error", applog.Scrub(err.Error()))
	}

	w.metrics.DeadLettered(msg.TenantID, msg.Channel)
	log.Error("message moved to dead letter after exhausting retries",
		"attempts", attempts, "reason", reason)
}

// awaitRateLimit waits until the binding is under its per-minute allowance.
//
// It waits rather than failing. The agent has already run and its tools have
// already had their effects, so dropping the reply would charge the tenant for
// an answer the user never sees. The wait is bounded so a permanently
// saturated binding surfaces as a delivery failure — which the sweeper then
// retries — instead of pinning a Worker slot forever.
func (w *Worker) awaitRateLimit(ctx context.Context, log *slog.Logger, binding *types.ChannelBinding) error {
	limit := binding.Capabilities.RateLimitPerMin
	if w.rateLimit == nil || limit <= 0 {
		return nil
	}

	scope := "outbound:" + binding.ChannelBindingID
	deadline := time.Now().Add(30 * time.Second)

	for {
		allowed, err := w.rateLimit.AllowRate(ctx, scope, limit)
		if err != nil {
			// A broken limiter must not block delivery. Proceeding risks the
			// platform's own throttle, which is recoverable; withholding the
			// reply is not.
			log.Warn("rate limiter unavailable, delivering anyway",
				"error", applog.Scrub(err.Error()))
			return nil
		}
		if allowed {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("outbound rate limit for %s not cleared within 30s",
				binding.ChannelBindingID)
		}

		log.Debug("outbound rate limited, waiting", "limit_per_min", limit)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// Redeliver sends an already-produced reply again, without re-running the
// agent.
//
// This is the sweeper's entry point for a delivery that failed after a
// successful run. It deliberately does not touch the Runtime, the mailbox or
// the lease: nothing is re-executed, only re-sent.
func (w *Worker) Redeliver(ctx context.Context, tenantID, sessionID, requestID, reply string) error {
	sess, err := w.store.SessionByID(ctx, tenantID, sessionID)
	if err != nil {
		return fmt.Errorf("load session for redelivery: %w", err)
	}

	rc := &types.RequestContext{
		TenantID:         sess.TenantID,
		AgentAppID:       sess.AgentAppID,
		AgentVersion:     sess.AgentVersion,
		Channel:          sess.Channel,
		ChannelBindingID: sess.ChannelBindingID,
		SessionID:        sess.SessionID,
		RequestID:        requestID,
	}
	ctx = types.NewContext(ctx, rc)
	log := applog.With(w.log, rc).With("worker_id", w.id, "redelivery", true)

	// The original inbound message is gone by now, so the delivery target is
	// derived from the session alone. That works because a direct session's
	// scope key *is* the external user id.
	err = w.deliver(ctx, log, sess, &types.InboundMessage{
		Channel:          sess.Channel,
		ChannelBindingID: sess.ChannelBindingID,
		TenantID:         sess.TenantID,
		ExternalUserID:   sess.ScopeKey,
		RequestID:        requestID,
	}, reply)
	w.metrics.Delivery(ctx, sess.Channel, err)
	return err
}
