package metrics

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

func testContext() context.Context {
	return types.NewContext(context.Background(), &types.RequestContext{
		TenantID:   "tenant-demo",
		AgentAppID: "assistant",
		Channel:    "mock",
		SessionID:  "sess-1",
		RequestID:  "req-1",
	})
}

func TestCounterAndSnapshot(t *testing.T) {
	reg := NewRegistry()
	reg.Inc(MetricRequests, Labels{Tenant: "t1", Outcome: OutcomeSuccess})
	reg.Inc(MetricRequests, Labels{Tenant: "t1", Outcome: OutcomeSuccess})
	reg.Inc(MetricRequests, Labels{Tenant: "t2", Outcome: OutcomeFailure})

	out := reg.Snapshot()
	if !strings.Contains(out, `agent_requests_total{tenant="t1",outcome="success"} 2`) {
		t.Errorf("t1 counter missing or wrong:\n%s", out)
	}
	if !strings.Contains(out, `agent_requests_total{tenant="t2",outcome="failure"} 1`) {
		t.Errorf("t2 counter missing or wrong:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE agent_requests_total counter") {
		t.Error("missing TYPE header")
	}
}

func TestTenantsGetSeparateSeries(t *testing.T) {
	// Aggregate numbers answer "is the platform healthy"; only per-tenant
	// numbers answer "which tenant is causing this".
	reg := NewRegistry()
	reg.Inc(MetricErrors, Labels{Tenant: "noisy", Outcome: "model_call"})
	reg.Inc(MetricErrors, Labels{Tenant: "quiet", Outcome: "model_call"})

	out := reg.Snapshot()
	if !strings.Contains(out, `tenant="noisy"`) || !strings.Contains(out, `tenant="quiet"`) {
		t.Errorf("tenants were collapsed into one series:\n%s", out)
	}
}

func TestHistogramBucketsAreCumulative(t *testing.T) {
	reg := NewRegistry()
	for _, ms := range []float64{3, 30, 300, 3000} {
		reg.Observe(MetricModelLatency, Labels{Tenant: "t1"}, ms)
	}

	out := reg.Snapshot()
	if !strings.Contains(out, `agent_model_latency_ms_count{tenant="t1"} 4`) {
		t.Errorf("count wrong:\n%s", out)
	}
	// 3ms falls in the le=5 bucket; cumulative counts mean le=5 holds exactly
	// one sample and le=+Inf holds all four.
	if !strings.Contains(out, `le="5"} 1`) {
		t.Errorf("le=5 bucket wrong:\n%s", out)
	}
	if !strings.Contains(out, `le="+Inf"} 4`) {
		t.Errorf("le=+Inf bucket should hold every sample:\n%s", out)
	}
}

func TestSeriesOverflowFoldsRatherThanDrops(t *testing.T) {
	// A label that turns out to be less bounded than expected must not grow
	// the series set without limit, but the totals still have to be correct.
	reg := NewRegistry()
	reg.maxSeries = 3

	for i := 0; i < 50; i++ {
		reg.Inc(MetricRequests, Labels{Tenant: "tenant-" + string(rune('a'+i%26)) + strconv.Itoa(i)})
	}

	out := reg.Snapshot()
	if !strings.Contains(out, `overflow="true"`) {
		t.Errorf("expected an overflow series:\n%s", out)
	}

	var total float64
	for _, v := range reg.counters[MetricRequests] {
		total += v
	}
	if total != 50 {
		t.Errorf("overflow lost samples: total = %g, want 50", total)
	}
}

func TestRecorderDerivesLabelsFromContext(t *testing.T) {
	rec := NewRecorder(NewRegistry())
	ctx := testContext()

	rec.AgentRun(ctx, time.Now(), nil)
	rec.ModelCall(ctx, "deepseek-chat", time.Now(), nil)
	rec.Tokens(ctx, "deepseek-chat", 120, 45, 0.0012)

	out := rec.Registry().Snapshot()
	for _, want := range []string{
		`tenant="tenant-demo"`,
		`agent="assistant"`,
		`model="deepseek-chat"`,
		"agent_tokens_total",
		"agent_cost_usd_total",
		"agent_model_latency_ms_count",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("snapshot missing %q:\n%s", want, out)
		}
	}
}

func TestRecorderToleratesContextWithoutTenant(t *testing.T) {
	// Metrics must never be the reason a request fails.
	rec := NewRecorder(NewRegistry())
	rec.AgentRun(context.Background(), time.Now(), nil)
	rec.ModelCall(context.Background(), "m", time.Now(), nil)
	if rec.Registry().Snapshot() == "" {
		t.Error("expected metrics to be recorded even without a tenant")
	}
}

func TestDenialIsNotCountedAsError(t *testing.T) {
	// A denial is policy working. Folding it into the error rate would make
	// the alarm fire on correct behaviour.
	rec := NewRecorder(NewRegistry())
	ctx := testContext()
	rec.ToolDenied(ctx, "delete_order")

	out := rec.Registry().Snapshot()
	if strings.Contains(out, "agent_errors_total") {
		t.Errorf("denial should not increment the error counter:\n%s", out)
	}
	if !strings.Contains(out, `outcome="denied"`) {
		t.Errorf("denial should be visible as its own outcome:\n%s", out)
	}
}

func TestDeliverySuccessAndFailureShareOneMetric(t *testing.T) {
	// A success rate has to be a ratio of the same series, or it cannot be
	// expressed as one query.
	rec := NewRecorder(NewRegistry())
	ctx := testContext()
	rec.Delivery(ctx, "mock", nil)
	rec.Delivery(ctx, "mock", errFake{})

	out := rec.Registry().Snapshot()
	if !strings.Contains(out, `agent_delivery_total{tenant="tenant-demo",agent="assistant",channel="mock",outcome="success"} 1`) {
		t.Errorf("success sample missing:\n%s", out)
	}
	if !strings.Contains(out, `outcome="failure"} 1`) {
		t.Errorf("failure sample missing:\n%s", out)
	}
}

type errFake struct{}

func (errFake) Error() string { return "boom" }
