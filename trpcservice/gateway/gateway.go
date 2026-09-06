// 设计依据：docs/IM通道接入设计.md §5「入站流程」
//                docs/多租户与节点部署设计.md §5「调度模型」

// Package gateway is the inbound half of the platform: it turns an untrusted
// IM callback into a queued, deduplicated unit of work.
//
// The order of the three critical steps is fixed and load-bearing:
//
//	idempotency record committed → ACK → enqueue
//
// Committing first means an in-flight request always has a durable record, so
// a hint lost by the queue can be recovered by sweeping rows stuck in
// processing. ACKing second stops the platform retrying while the agent is
// still working. Enqueueing last means a queue failure costs a delay, not a
// lost message.
package gateway

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/attribute"

	"github.com/liuzengh/trpc-agent-service/trpcservice/admin"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/metrics"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/telemetry"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
	"github.com/liuzengh/trpc-agent-service/trpcservice/web"
)

// Deps are the collaborators a Gateway needs.
type Deps struct {
	Config     *config.Config
	Store      *store.Store
	Dispatcher types.SessionDispatcher
	Mailbox    types.SessionMailbox
	Channels   *channels.Registry
	Metrics    *metrics.Recorder
	Tracer     *telemetry.Provider
	DeadLetter admin.DeadLetterStore
	Logger     *slog.Logger
}

// Gateway serves inbound callbacks.
type Gateway struct {
	cfg        *config.Config
	store      *store.Store
	dispatcher types.SessionDispatcher
	mailbox    types.SessionMailbox
	channels   *channels.Registry
	metrics    *metrics.Recorder
	tracer     *telemetry.Provider
	deadLetter admin.DeadLetterStore
	log        *slog.Logger
}

// New builds a Gateway.
func New(d Deps) (*Gateway, error) {
	switch {
	case d.Config == nil:
		return nil, errors.New("gateway: config is required")
	case d.Store == nil:
		return nil, errors.New("gateway: store is required")
	case d.Dispatcher == nil:
		return nil, errors.New("gateway: dispatcher is required")
	case d.Mailbox == nil:
		return nil, errors.New("gateway: mailbox is required")
	case d.Channels == nil:
		return nil, errors.New("gateway: channel registry is required")
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	rec := d.Metrics
	if rec == nil {
		// A no-op recorder rather than nil checks at every call site:
		// instrumentation must never be the reason a request fails.
		rec = metrics.NewRecorder(metrics.NewRegistry())
	}
	return &Gateway{
		cfg: d.Config, store: d.Store, dispatcher: d.Dispatcher,
		mailbox: d.Mailbox, channels: d.Channels, metrics: rec,
		tracer: d.Tracer, deadLetter: d.DeadLetter, log: logger,
	}, nil
}

// Router builds the HTTP routes.
//
// Webhook paths are not registered individually: bindings live in the
// database and change without a restart, so one catch-all route resolves the
// binding by path at request time.
func (g *Gateway) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	r.GET("/healthz", g.handleHealth)
	r.GET("/metrics", gin.WrapF(g.metrics.Registry().Handler()))

	// The control-plane API lives here because Gateway is already the process
	// with a public listener and a database connection; Workers expose
	// nothing callable.
	admin.New(g.store, g.deadLetter, g.log).Register(r)

	// The operator console. It posts to the real webhook endpoint rather than
	// to a private shortcut, so a working console is itself evidence the
	// platform works.
	web.New(g.store, g.log).Register(r)

	r.Any("/webhook/*path", g.handleWebhook)
	return r
}

func (g *Gateway) handleHealth(c *gin.Context) {
	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
	defer cancel()

	if err := g.store.Ping(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "degraded", "mysql": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

// handleWebhook is the single entry point for every channel.
func (g *Gateway) handleWebhook(c *gin.Context) {
	// The span opens before anything else, so even a request rejected at the
	// binding lookup shows up in the trace. A callback that 404s is exactly
	// the kind of thing someone comes to a trace to explain.
	//
	// Any upstream traceparent is honoured, so a trace started at the IM
	// platform's edge continues here rather than being replaced.
	ctx := telemetry.Extract(c.Request.Context(), headerCarrier(c.Request))
	ctx, span := g.tracer.StartSpan(ctx, "gateway.inbound",
		attribute.String("http.route", c.Request.URL.Path),
		attribute.String("http.method", c.Request.Method))
	defer span.End()

	// The trace id comes from the span, not from a fresh uuid: a log line and
	// a span have to carry the same value or they cannot be joined.
	traceID := telemetry.TraceIDFrom(ctx)
	if traceID == "" {
		// Tracing is disabled, so no span context exists. Fall back to a
		// generated id — logs still need something to correlate on.
		traceID = "trace-" + uuid.NewString()
	}

	// 1. Resolve the binding. This is where the request acquires its tenant:
	//    the payload is untrusted and must not be able to name its own.
	binding, err := g.store.ChannelBindingByWebhook(ctx, c.Request.URL.Path)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			span.SetAttributes(attribute.String("reject.reason", "unknown_webhook"))
			g.log.Warn("unknown webhook path", "path", c.Request.URL.Path, "trace_id", traceID)
			c.JSON(http.StatusNotFound, gin.H{"error": "unknown webhook"})
			return
		}
		g.log.Error("resolve binding failed", "path", c.Request.URL.Path,
			"trace_id", traceID, "error", applog.Scrub(err.Error()))
		c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
		return
	}

	span.SetAttributes(
		attribute.String("tenant.id", binding.TenantID),
		attribute.String("agent.app_id", binding.AgentAppID),
		attribute.String("channel", binding.Channel),
		attribute.String("channel.binding_id", binding.ChannelBindingID))

	log := g.log.With("tenant_id", binding.TenantID, "agent_app_id", binding.AgentAppID,
		"channel", binding.Channel, "channel_binding_id", binding.ChannelBindingID,
		"trace_id", traceID)

	ch, err := g.channels.Inbound(binding.Channel)
	if err != nil {
		log.Error("no channel implementation", "error", err.Error())
		c.JSON(http.StatusInternalServerError, gin.H{"error": "channel unavailable"})
		return
	}

	// 2. Verify before decoding: an unverified body may be attacker-supplied.
	if err := ch.Verify(c.Request, binding); err != nil {
		log.Warn("verification failed", "error", applog.Scrub(err.Error()))
		telemetry.RecordError(span, err)
		g.metrics.InboundRejected(ctx, binding.Channel, binding.TenantID, "verification")
		c.JSON(http.StatusUnauthorized, gin.H{"error": "verification failed"})
		return
	}

	// 3. Decode. Fetch-mode channels pull a batch here, so this is a slice.
	messages, err := ch.Decode(ctx, c.Request, binding)
	if err != nil {
		log.Warn("decode failed", "error", applog.Scrub(err.Error()))
		telemetry.RecordError(span, err)
		g.metrics.InboundRejected(ctx, binding.Channel, binding.TenantID, "decode")
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed payload"})
		return
	}
	if len(messages) == 0 {
		// A URL-verification handshake or an unrelated event type decodes to
		// nothing. ACK so the platform stops retrying.
		log.Debug("callback carried no messages")
		_ = ch.Ack(c.Writer, c.Request, binding, types.AckInfo{TraceID: traceID})
		return
	}

	// 4. Per message: locate the session and commit the idempotency record.
	accepted := make([]acceptedMessage, 0, len(messages))
	var lastInfo types.AckInfo
	lastInfo.TraceID = traceID

	for i := range messages {
		msg := &messages[i]
		msg.TraceID = traceID
		msg.RequestID = "req-" + uuid.NewString()

		info, item, err := g.admit(ctx, log, binding, msg)
		if err != nil {
			// The idempotency record could not be committed, so we do not
			// know whether this message was already handled. Failing closed
			// and letting the platform redeliver is the only safe choice:
			// a redelivery is caught by uk_event, a dropped message is not.
			log.Error("admit failed", "error", applog.Scrub(err.Error()))
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
			return
		}
		lastInfo = info
		if item != nil {
			accepted = append(accepted, *item)
		}
	}

	// 5. ACK. The agent has not run; the answer arrives later through a push.
	if err := ch.Ack(c.Writer, c.Request, binding, lastInfo); err != nil {
		log.Error("ack failed", "error", applog.Scrub(err.Error()))
	}

	// 6. Enqueue, after the ACK.
	//
	// The HTTP context is detached first: the client already has its response
	// and may disconnect immediately, which would cancel the request context
	// and abort the enqueue of work we have already promised to do.
	if len(accepted) > 0 {
		g.enqueue(context.WithoutCancel(ctx), log, accepted)
	}
}

// acceptedMessage is a message that has been recorded and needs queueing.
type acceptedMessage struct {
	msg  *types.InboundMessage
	hint types.SessionHint
}

// admit locates the session and writes the idempotency record.
//
// The returned item is nil for a duplicate: the record already existed, so the
// message must not be queued a second time.
func (g *Gateway) admit(
	ctx context.Context,
	log *slog.Logger,
	binding *types.ChannelBinding,
	msg *types.InboundMessage,
) (types.AckInfo, *acceptedMessage, error) {
	sess, created, err := g.store.FindOrCreateSession(ctx, store.SessionLookup{
		Binding:        binding,
		Scope:          msg.Scope,
		ScopeKey:       msg.ScopeKey,
		InternalUserID: internalUserID(msg),
	})
	if err != nil {
		return types.AckInfo{}, nil, err
	}
	if created {
		// Version selection happens once per session and is then frozen, so
		// this line is the only place a conversation's configuration is
		// decided. Worth logging.
		log.Info("session created", "session_id", sess.SessionID,
			"agent_version", sess.AgentVersion, "scope", sess.Scope)
	}

	// The full request identity, assembled once and carried from here on.
	rc := &types.RequestContext{
		TenantID:         sess.TenantID,
		AgentAppID:       sess.AgentAppID,
		AgentVersion:     sess.AgentVersion,
		Channel:          binding.Channel,
		ChannelBindingID: binding.ChannelBindingID,
		UserID:           msg.ExternalUserID,
		SessionID:        sess.SessionID,
		RequestID:        msg.RequestID,
		TraceID:          msg.TraceID,
	}

	inserted, err := g.store.InsertInboundEvent(ctx, &types.InboundEvent{
		TenantID:         rc.TenantID,
		ChannelBindingID: rc.ChannelBindingID,
		ExternalEventID:  msg.ExternalEventID,
		RequestID:        rc.RequestID,
		TraceID:          rc.TraceID,
		SessionID:        rc.SessionID,
		Payload:          msg,
		State:            types.StateProcessing,
	})
	if err != nil {
		return types.AckInfo{}, nil, err
	}

	info := types.AckInfo{
		RequestID: rc.RequestID,
		TraceID:   rc.TraceID,
		SessionID: rc.SessionID,
	}

	if !inserted {
		// The platform redelivered a message we already have. This is normal,
		// not an error: ACK and do nothing else, so the agent and its tools
		// do not run twice.
		info.Duplicate = true
		log.Info("duplicate inbound event ignored",
			"session_id", rc.SessionID, "external_event_id", msg.ExternalEventID)
		return info, nil, nil
	}

	return info, &acceptedMessage{
		msg: msg,
		hint: types.SessionHint{
			TenantID:     rc.TenantID,
			AgentAppID:   rc.AgentAppID,
			AgentVersion: rc.AgentVersion,
			SessionID:    rc.SessionID,
			TraceID:      rc.TraceID,
			// The W3C context, not just the id: it is what lets the Worker's
			// spans hang off this one instead of forming a second tree.
			TraceContext: telemetry.Inject(ctx),
		},
	}, nil
}

// enqueue writes each message to its session mailbox and publishes a hint.
//
// Mailbox first, then hint. The mailbox is what preserves order and holds the
// payload; the hint only says "there is work". Publishing first would let a
// Worker win the lease, find an empty mailbox and release it again — harmless
// but wasteful, and it would make an empty drain ambiguous.
func (g *Gateway) enqueue(ctx context.Context, log *slog.Logger, items []acceptedMessage) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	for _, item := range items {
		l := log.With("session_id", item.hint.SessionID, "request_id", item.msg.RequestID)

		if err := g.mailbox.Push(ctx, item.hint.SessionID, item.msg); err != nil {
			// The inbound_events row stays in processing, so a sweep can
			// replay it. Nothing is lost, but it will be late.
			l.Error("mailbox push failed", "error", applog.Scrub(err.Error()))
			continue
		}
		if err := g.dispatcher.Publish(ctx, item.hint); err != nil {
			// The message is safely in the mailbox; only the wake-up was
			// lost. A later hint for the same session, or a sweep, picks it
			// up — which is exactly why the queue is allowed to be lossy.
			l.Error("publish hint failed", "error", applog.Scrub(err.Error()))
			continue
		}
		g.metrics.InboundReceived(ctx, item.msg.Channel, item.hint.TenantID)
		l.Info("message queued")
	}
}

// internalUserID maps the external identity to an internal one.
//
// Phase one uses the external id directly. Group sessions get no user id at
// all: the participants are many, and the speaker belongs on each event
// rather than on the session.
func internalUserID(msg *types.InboundMessage) string {
	if msg.Scope == types.ScopeGroup {
		return ""
	}
	return msg.ExternalUserID
}

// headerCarrier exposes the request headers as a trace carrier, so an
// upstream traceparent continues into this trace instead of being discarded.
func headerCarrier(r *http.Request) telemetry.Carrier {
	c := make(telemetry.Carrier, 2)
	for _, h := range []string{"traceparent", "tracestate"} {
		if v := r.Header.Get(h); v != "" {
			c[h] = v
		}
	}
	return c
}
