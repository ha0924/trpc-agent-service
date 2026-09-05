// 设计依据：docs/技术设计方案.md §7.3「Metrics、Trace 和审计」
//                docs/治理监控与安全设计.md

// Package metrics records the platform's operational indicators and exposes
// them in Prometheus text format.
//
// Every metric carries a tenant label. Aggregate numbers answer "is the
// platform healthy"; only per-tenant numbers answer "which tenant is causing
// this", which is the question that actually gets asked during an incident on
// a shared platform.
//
// The registry is deliberately self-contained rather than built on the
// OpenTelemetry SDK. What must not be deferred is the *instrumentation* —
// where a measurement is taken and what labels it carries — because that is
// spread across every package and expensive to retrofit. The exporter behind
// it is one file. Swapping this registry for an OTel meter later changes
// Recorder's implementation and nothing that calls it.
//
// Cardinality is the standing hazard: a label whose values are unbounded
// (session id, request id, user id) would grow the series set without limit.
// Only tenant, agent, channel, model, tool and outcome appear as labels, all
// of which are bounded by configuration rather than by traffic.
package metrics

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Metric names. Grouped to mirror the indicators the platform is required to
// report: request volume, model latency, tool latency, delivery success,
// error rate, token consumption, per-tenant cost and session backend latency.
const (
	MetricRequests        = "agent_requests_total"
	MetricErrors          = "agent_errors_total"
	MetricModelLatency    = "agent_model_latency_ms"
	MetricToolLatency     = "agent_tool_latency_ms"
	MetricDelivery        = "agent_delivery_total"
	MetricTokens          = "agent_tokens_total"
	MetricCost            = "agent_cost_usd_total"
	MetricStorageLatency  = "agent_storage_latency_ms"
	MetricQueueDepth      = "agent_queue_depth"
	MetricRuntimeCached   = "agent_runtime_cached"
	MetricLeaseContention = "agent_lease_contention_total"
	MetricAuditDropped    = "agent_audit_dropped_total"
	MetricDeadLettered    = "agent_dead_lettered_total"
)

// Labels identify one time series. Only bounded dimensions belong here.
type Labels struct {
	Tenant  string
	Agent   string
	Channel string
	Model   string
	Tool    string
	// Outcome is success / failure / denied, so an error rate can be derived
	// from the same series as the total.
	Outcome string
}

// key renders the labels into a stable series identifier.
func (l Labels) key() string {
	parts := make([]string, 0, 6)
	add := func(name, v string) {
		if v != "" {
			parts = append(parts, name+"="+strconv.Quote(v))
		}
	}
	add("tenant", l.Tenant)
	add("agent", l.Agent)
	add("channel", l.Channel)
	add("model", l.Model)
	add("tool", l.Tool)
	add("outcome", l.Outcome)
	return strings.Join(parts, ",")
}

// Outcome values.
const (
	OutcomeSuccess = "success"
	OutcomeFailure = "failure"
	OutcomeDenied  = "denied"
)

// Registry collects metrics.
type Registry struct {
	mu         sync.RWMutex
	counters   map[string]map[string]float64
	gauges     map[string]map[string]float64
	histograms map[string]map[string]*histogram

	// maxSeries bounds the series set per metric. Past the bound new label
	// combinations are folded into an "overflow" series rather than being
	// dropped silently, so the numbers stay correct in aggregate even when a
	// label turns out to be less bounded than expected.
	maxSeries int
	overflow  map[string]bool
}

// histogram accumulates a latency distribution in fixed buckets.
//
// Buckets rather than raw values: a request-rate-sized slice of durations
// would grow without limit, and percentiles are what get looked at anyway.
type histogram struct {
	buckets []float64
	counts  []uint64
	sum     float64
	total   uint64
}

// latencyBuckets are millisecond boundaries chosen for this workload. Model
// calls sit in the hundreds-to-thousands range, storage in single-digit
// milliseconds, so the scale spans both.
var latencyBuckets = []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000}

func newHistogram() *histogram {
	return &histogram{
		buckets: latencyBuckets,
		counts:  make([]uint64, len(latencyBuckets)+1), // +1 for +Inf
	}
}

func (h *histogram) observe(v float64) {
	h.sum += v
	h.total++
	for i, b := range h.buckets {
		if v <= b {
			h.counts[i]++
			return
		}
	}
	h.counts[len(h.buckets)]++
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry {
	return &Registry{
		counters:   make(map[string]map[string]float64),
		gauges:     make(map[string]map[string]float64),
		histograms: make(map[string]map[string]*histogram),
		maxSeries:  2000,
		overflow:   make(map[string]bool),
	}
}

// series returns the label key to use, folding into "overflow" once the
// per-metric series budget is exhausted.
func (r *Registry) series(existing int, metric, key string) string {
	if existing < r.maxSeries {
		return key
	}
	r.overflow[metric] = true
	return `overflow="true"`
}

// Inc adds one to a counter.
func (r *Registry) Inc(name string, l Labels) { r.Add(name, l, 1) }

// Add increases a counter.
func (r *Registry) Add(name string, l Labels, delta float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.counters[name]
	if !ok {
		m = make(map[string]float64)
		r.counters[name] = m
	}
	key := l.key()
	if _, seen := m[key]; !seen {
		key = r.series(len(m), name, key)
	}
	m[key] += delta
}

// Set records a gauge value.
func (r *Registry) Set(name string, l Labels, v float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.gauges[name]
	if !ok {
		m = make(map[string]float64)
		r.gauges[name] = m
	}
	m[l.key()] = v
}

// Observe records a latency sample in milliseconds.
func (r *Registry) Observe(name string, l Labels, ms float64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m, ok := r.histograms[name]
	if !ok {
		m = make(map[string]*histogram)
		r.histograms[name] = m
	}
	key := l.key()
	h, ok := m[key]
	if !ok {
		key = r.series(len(m), name, key)
		h, ok = m[key]
		if !ok {
			h = newHistogram()
			m[key] = h
		}
	}
	h.observe(ms)
}

// ObserveSince records the time elapsed since start.
func (r *Registry) ObserveSince(name string, l Labels, start time.Time) {
	r.Observe(name, l, float64(time.Since(start).Microseconds())/1000.0)
}

// Snapshot renders every metric in Prometheus text exposition format.
func (r *Registry) Snapshot() string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	var b strings.Builder
	for _, name := range sortedKeys(r.counters) {
		fmt.Fprintf(&b, "# TYPE %s counter\n", name)
		writeSamples(&b, name, "", r.counters[name])
	}
	for _, name := range sortedKeys(r.gauges) {
		fmt.Fprintf(&b, "# TYPE %s gauge\n", name)
		writeSamples(&b, name, "", r.gauges[name])
	}
	for _, name := range sortedHistKeys(r.histograms) {
		fmt.Fprintf(&b, "# TYPE %s histogram\n", name)
		series := r.histograms[name]
		for _, key := range sortedHistSeries(series) {
			h := series[key]
			cumulative := uint64(0)
			for i, bound := range h.buckets {
				cumulative += h.counts[i]
				fmt.Fprintf(&b, "%s_bucket{%s} %d\n", name,
					joinLabels(key, fmt.Sprintf(`le="%g"`, bound)), cumulative)
			}
			cumulative += h.counts[len(h.buckets)]
			fmt.Fprintf(&b, "%s_bucket{%s} %d\n", name, joinLabels(key, `le="+Inf"`), cumulative)
			fmt.Fprintf(&b, "%s_sum{%s} %g\n", name, key, h.sum)
			fmt.Fprintf(&b, "%s_count{%s} %d\n", name, key, h.total)
		}
	}
	return b.String()
}

// Handler serves the snapshot.
func (r *Registry) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		fmt.Fprint(w, r.Snapshot())
	}
}

func writeSamples(b *strings.Builder, name, extra string, series map[string]float64) {
	for _, key := range sortedFloatSeries(series) {
		labels := joinLabels(key, extra)
		if labels == "" {
			fmt.Fprintf(b, "%s %g\n", name, series[key])
			continue
		}
		fmt.Fprintf(b, "%s{%s} %g\n", name, labels, series[key])
	}
}

func joinLabels(a, b string) string {
	switch {
	case a == "":
		return b
	case b == "":
		return a
	default:
		return a + "," + b
	}
}

func sortedKeys(m map[string]map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedHistKeys(m map[string]map[string]*histogram) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedFloatSeries(m map[string]float64) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func sortedHistSeries(m map[string]*histogram) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
