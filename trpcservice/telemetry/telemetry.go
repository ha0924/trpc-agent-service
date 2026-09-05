// 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」
//                docs/治理监控与安全设计.md

// Package telemetry wires OpenTelemetry tracing across both processes.
//
// The framework already instruments its own execution — Runner, model calls,
// tool calls — through telemetry/trace. Installing a real tracer provider is
// therefore enough to get those spans; this package's job is the part the
// framework cannot do:
//
//   - Carry trace context across the queue. Gateway and Worker are separate
//     processes with a queue between them, and a trace id alone is not enough:
//     without the full W3C context the Worker starts a new root span and the
//     result is two disconnected trees rather than one.
//   - Attach tenant identity to every span, so a trace is attributable on a
//     shared platform.
//   - Derive the log-visible trace id from the span context, so a log line and
//     a span can be joined by the same value.
//
// Sampling and export are configuration. Instrumentation is not: where a span
// starts and what it carries is spread across every package, which is why it
// is worth getting right now even though the collector is optional.
package telemetry

import (
	"context"
	"fmt"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
	frametrace "trpc.group/trpc-go/trpc-agent-go/telemetry/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// tracerName identifies spans this platform creates, as opposed to the ones
// the framework emits from inside Runner.
const tracerName = "trpc-agent-platform"

// Provider owns the tracer lifecycle.
type Provider struct {
	tracer   oteltrace.Tracer
	shutdown func() error
	enabled  bool
	log      *slog.Logger
}

// Start installs a tracer provider and returns a Provider.
//
// When telemetry is disabled it returns a working Provider backed by the
// framework's default no-op tracer. Call sites therefore never branch on
// whether tracing is on — instrumentation that has to be guarded at every
// call site is instrumentation that rots.
func Start(ctx context.Context, cfg *config.Config, serviceName string, logger *slog.Logger) (*Provider, error) {
	if logger == nil {
		logger = slog.Default()
	}

	// The W3C propagator is installed regardless. Context propagation across
	// the queue must work even with export disabled, otherwise turning
	// telemetry on in production would be the first time the plumbing is
	// exercised.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	p := &Provider{
		tracer: otel.Tracer(tracerName),
		log:    logger,
	}

	if !cfg.Telemetry.Enabled {
		logger.Info("tracing disabled; spans are no-ops but context still propagates")
		return p, nil
	}

	opts := []frametrace.Option{
		frametrace.WithServiceName(serviceName),
		frametrace.WithServiceVersion(cfg.Telemetry.ServiceVersion),
		frametrace.WithServiceNamespace(cfg.Telemetry.ServiceNamespace),
	}
	if cfg.Telemetry.Endpoint != "" {
		opts = append(opts, frametrace.WithEndpoint(cfg.Telemetry.Endpoint))
	}
	if cfg.Telemetry.Protocol != "" {
		opts = append(opts, frametrace.WithProtocol(cfg.Telemetry.Protocol))
	}

	clean, err := frametrace.Start(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("start tracing: %w", err)
	}

	// Re-read the tracer from the OTel global registry, not from the
	// framework's own package variable.
	//
	// frametrace.Start installs the provider via otel.SetTracerProvider but
	// leaves its exported frametrace.TracerProvider as the original no-op —
	// it is never reassigned. Reading from there yields a tracer that
	// silently drops every span, and the symptom is subtle: the framework's
	// own spans arrive because its internals use the global, while the
	// platform's spans vanish. Found by observing real OTLP output.
	p.tracer = otel.Tracer(tracerName)
	p.shutdown = clean
	p.enabled = true

	logger.Info("tracing enabled",
		"service", serviceName,
		"endpoint", cfg.Telemetry.Endpoint,
		"protocol", cfg.Telemetry.Protocol)
	return p, nil
}

// Enabled reports whether spans are actually exported.
func (p *Provider) Enabled() bool { return p != nil && p.enabled }

// Tracer returns the platform tracer.
func (p *Provider) Tracer() oteltrace.Tracer {
	if p == nil {
		return otel.Tracer(tracerName)
	}
	return p.tracer
}

// Shutdown flushes pending spans.
//
// Worth waiting for: the spans describing a failure are the ones still in the
// buffer when the process is asked to stop.
func (p *Provider) Shutdown() error {
	if p == nil || p.shutdown == nil {
		return nil
	}
	return p.shutdown()
}

// StartSpan begins a span carrying the request's tenant identity.
//
// Tenant, agent and channel become attributes; session and request ids do
// too, because unlike metrics a span is one event rather than a time series,
// so high-cardinality values cost nothing here and are exactly what makes a
// single conversation findable.
func (p *Provider) StartSpan(ctx context.Context, name string, extra ...attribute.KeyValue) (context.Context, oteltrace.Span) {
	attrs := extra
	if rc, err := types.FromContext(ctx); err == nil {
		attrs = append(attrs,
			attribute.String("tenant.id", rc.TenantID),
			attribute.String("agent.app_id", rc.AgentAppID),
			attribute.String("agent.version", rc.AgentVersion),
			attribute.String("channel", rc.Channel),
			attribute.String("session.id", rc.SessionID),
			attribute.String("request.id", rc.RequestID),
		)
	}
	return p.Tracer().Start(ctx, name, oteltrace.WithAttributes(attrs...))
}

// Carrier is a map-backed TextMapCarrier for trace context that travels
// through a queue rather than through HTTP headers.
type Carrier map[string]string

var _ propagation.TextMapCarrier = Carrier(nil)

func (c Carrier) Get(key string) string { return c[key] }
func (c Carrier) Set(key, value string) { c[key] = value }

func (c Carrier) Keys() []string {
	out := make([]string, 0, len(c))
	for k := range c {
		out = append(out, k)
	}
	return out
}

// Inject writes the current trace context into a carrier.
//
// This is what makes the Worker's spans children of the Gateway's rather than
// roots of a second tree. A trace id copied as a plain string is not enough:
// the span id and the sampling decision travel in the same header, and
// without the span id there is nothing to be a child of.
func Inject(ctx context.Context) Carrier {
	c := make(Carrier, 2)
	otel.GetTextMapPropagator().Inject(ctx, c)
	return c
}

// Extract restores trace context from a carrier.
//
// An absent or malformed carrier yields ctx unchanged, so the consumer starts
// a root span instead of failing. A lost trace is an observability gap; a
// failed message is an outage.
func Extract(ctx context.Context, c Carrier) context.Context {
	if len(c) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, c)
}

// TraceIDFrom returns the trace id of the span in ctx, or "" when none.
//
// Used to seed RequestContext.TraceID so that the value in a log line is the
// same value that identifies the trace. Two independently generated ids —
// one for logs, one for traces — cannot be joined, which defeats the purpose
// of having either.
func TraceIDFrom(ctx context.Context) string {
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}

// SpanIDFrom returns the span id in ctx, for diagnostics.
func SpanIDFrom(ctx context.Context) string {
	sc := oteltrace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.SpanID().String()
}

// RecordError marks a span failed.
//
// Both calls are needed: RecordError attaches the message, SetStatus is what
// makes the span show as an error in a backend's UI. Doing only the first
// produces traces that look healthy while carrying error events.
func RecordError(span oteltrace.Span, err error) {
	if span == nil || err == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
