package telemetry

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	oteltrace "go.opentelemetry.io/otel/trace"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// newRecorder installs a real SDK tracer writing to memory, so the tests
// assert on actual span relationships rather than on whether a function was
// called. A no-op tracer would let a broken parent link pass silently.
func newRecorder(t *testing.T) (*tracetest.SpanRecorder, oteltrace.Tracer) {
	t.Helper()

	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return rec, tp.Tracer("test")
}

func TestInjectExtractMakesWorkerSpanAChild(t *testing.T) {
	// This is the property the whole package exists for. Gateway and Worker
	// are separate processes with a queue between them; if the context does
	// not survive the hop, one message produces two disconnected traces.
	rec, tracer := newRecorder(t)

	// Gateway side.
	gwCtx, gwSpan := tracer.Start(context.Background(), "gateway.inbound")
	carrier := Inject(gwCtx)
	gwSpan.End()

	if carrier["traceparent"] == "" {
		t.Fatal("Inject wrote no traceparent; the hop cannot be joined")
	}

	// Worker side, in what would be a different process.
	wkCtx := Extract(context.Background(), carrier)
	_, wkSpan := tracer.Start(wkCtx, "worker.execute")
	wkSpan.End()

	spans := rec.Ended()
	if len(spans) != 2 {
		t.Fatalf("recorded %d spans, want 2", len(spans))
	}

	var gw, wk sdktrace.ReadOnlySpan
	for _, s := range spans {
		switch s.Name() {
		case "gateway.inbound":
			gw = s
		case "worker.execute":
			wk = s
		}
	}
	if gw == nil || wk == nil {
		t.Fatal("expected one span from each side")
	}

	if gw.SpanContext().TraceID() != wk.SpanContext().TraceID() {
		t.Errorf("trace ids differ, so the two processes produced separate traces:\n gateway %s\n worker  %s",
			gw.SpanContext().TraceID(), wk.SpanContext().TraceID())
	}
	// The parent link is what a trace id alone cannot give: without the
	// parent span id the worker span is a second root.
	if wk.Parent().SpanID() != gw.SpanContext().SpanID() {
		t.Errorf("worker span is not a child of the gateway span:\n parent %s\n want   %s",
			wk.Parent().SpanID(), gw.SpanContext().SpanID())
	}
	if !wk.Parent().IsRemote() {
		t.Error("parent should be marked remote; it came from another process")
	}
}

func TestSamplingDecisionCrossesTheHop(t *testing.T) {
	// The sampling flag travels in traceparent. If it were dropped, the
	// Worker would sample independently and half a trace would be missing —
	// worse than no trace, because it looks complete.
	_, tracer := newRecorder(t)

	ctx, span := tracer.Start(context.Background(), "gateway.inbound")
	sampledUpstream := span.SpanContext().IsSampled()
	carrier := Inject(ctx)
	span.End()

	restored := Extract(context.Background(), carrier)
	sc := oteltrace.SpanContextFromContext(restored)
	if sc.IsSampled() != sampledUpstream {
		t.Errorf("sampling decision changed across the hop: %v then %v",
			sampledUpstream, sc.IsSampled())
	}
}

func TestExtractToleratesMissingAndMalformedCarrier(t *testing.T) {
	// A lost trace is an observability gap; a failed message is an outage.
	// Extraction must never be the reason a message fails.
	newRecorder(t)

	for name, carrier := range map[string]Carrier{
		"nil":       nil,
		"empty":     {},
		"garbage":   {"traceparent": "not-a-valid-traceparent"},
		"truncated": {"traceparent": "00-4bf92f35"},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := Extract(context.Background(), carrier)
			if ctx == nil {
				t.Fatal("Extract returned a nil context")
			}
			// A root span is the correct fallback, not a panic.
			if sc := oteltrace.SpanContextFromContext(ctx); sc.IsValid() {
				t.Errorf("malformed carrier produced a valid span context: %v", sc)
			}
		})
	}
}

func TestTraceIDFromMatchesTheSpan(t *testing.T) {
	// The trace id written to logs has to be the same value that identifies
	// the trace. Two independently generated ids cannot be joined, which
	// defeats the purpose of having either.
	_, tracer := newRecorder(t)

	ctx, span := tracer.Start(context.Background(), "op")
	defer span.End()

	got := TraceIDFrom(ctx)
	want := span.SpanContext().TraceID().String()
	if got != want {
		t.Errorf("TraceIDFrom = %q, want %q", got, want)
	}
	if SpanIDFrom(ctx) != span.SpanContext().SpanID().String() {
		t.Error("SpanIDFrom does not match the active span")
	}
}

func TestTraceIDFromEmptyContext(t *testing.T) {
	if got := TraceIDFrom(context.Background()); got != "" {
		t.Errorf("want empty string with no span, got %q", got)
	}
}

func TestStartSpanCarriesTenantIdentity(t *testing.T) {
	// A trace on a shared platform is only useful if it says whose it is.
	rec, tracer := newRecorder(t)
	p := &Provider{tracer: tracer}

	ctx := types.NewContext(context.Background(), &types.RequestContext{
		TenantID:     "tenant-demo",
		AgentAppID:   "assistant",
		AgentVersion: "v1",
		Channel:      "wecom",
		SessionID:    "sess-1",
		RequestID:    "req-1",
	})

	_, span := p.StartSpan(ctx, "worker.process")
	span.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans, want 1", len(spans))
	}

	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.AsString()
	}
	for key, want := range map[string]string{
		"tenant.id":     "tenant-demo",
		"agent.app_id":  "assistant",
		"agent.version": "v1",
		"channel":       "wecom",
		"session.id":    "sess-1",
		"request.id":    "req-1",
	} {
		if attrs[key] != want {
			t.Errorf("attribute %s = %q, want %q", key, attrs[key], want)
		}
	}
}

func TestStartSpanWithoutTenantContext(t *testing.T) {
	// Instrumentation must not be the reason a request fails.
	rec, tracer := newRecorder(t)
	p := &Provider{tracer: tracer}

	_, span := p.StartSpan(context.Background(), "orphan")
	span.End()

	if len(rec.Ended()) != 1 {
		t.Fatal("span should still be recorded without a tenant")
	}
}

func TestCarrierImplementsTextMapCarrier(t *testing.T) {
	c := Carrier{}
	c.Set("traceparent", "value")
	if c.Get("traceparent") != "value" {
		t.Error("Get did not return what Set stored")
	}
	if keys := c.Keys(); len(keys) != 1 || keys[0] != "traceparent" {
		t.Errorf("Keys = %v", keys)
	}
}

func TestRecordErrorSetsStatus(t *testing.T) {
	// RecordError alone attaches the message but leaves the span looking
	// healthy in a backend's UI; SetStatus is what marks it failed.
	rec, tracer := newRecorder(t)

	_, span := tracer.Start(context.Background(), "failing")
	RecordError(span, errBoom{})
	span.End()

	spans := rec.Ended()
	if len(spans) != 1 {
		t.Fatalf("recorded %d spans", len(spans))
	}
	if spans[0].Status().Code.String() != "Error" {
		t.Errorf("status = %s, want Error", spans[0].Status().Code)
	}
	if len(spans[0].Events()) == 0 {
		t.Error("expected an exception event on the span")
	}
}

func TestRecordErrorIgnoresNil(t *testing.T) {
	_, tracer := newRecorder(t)
	_, span := tracer.Start(context.Background(), "ok")
	RecordError(span, nil)
	RecordError(nil, errBoom{})
	span.End()
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }
