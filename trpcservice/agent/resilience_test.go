package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"
)

// These tests cover the resilience wrapper. 风险清单 #6 named four mitigations
// for provider failure — timeout, bounded retry, fallback, breaker — and only
// the first existed. The properties worth pinning are the ones where a
// plausible implementation does the wrong thing:
//
//   - a non-retryable error must not be retried, or a wrong API key costs the
//     user several timeouts instead of one fast failure;
//   - cancellation must not count toward the breaker, or a burst of user
//     cancellations opens the circuit on a healthy provider;
//   - an open circuit must fail fast, which is the whole point: a dead
//     provider with a 30s timeout otherwise holds every Worker slot.

// fakeModel returns queued outcomes in order.
type fakeModel struct {
	mu    sync.Mutex
	errs  []error
	calls int
	info  model.Info
}

func (f *fakeModel) GenerateContent(ctx context.Context, req *model.Request) (<-chan *model.Response, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++

	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	if err != nil {
		return nil, err
	}
	ch := make(chan *model.Response, 1)
	ch <- &model.Response{}
	close(ch)
	return ch, nil
}

func (f *fakeModel) Info() model.Info { return f.info }

func (f *fakeModel) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fastConfig keeps tests quick while still exercising the retry path.
func fastConfig(attempts int) ResilienceConfig {
	return ResilienceConfig{
		MaxAttempts:      attempts,
		Backoff:          time.Millisecond,
		BreakerThreshold: 0, // breaker off unless a test wants it
	}
}

func TestSuccessOnFirstAttemptCallsOnce(t *testing.T) {
	primary := &fakeModel{}
	m := newResilientModel(primary, nil, "test", fastConfig(3), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err != nil {
		t.Fatalf("GenerateContent: %v", err)
	}
	if primary.callCount() != 1 {
		t.Errorf("called %d times, want 1: a success must not retry", primary.callCount())
	}
}

func TestRetryableErrorIsRetried(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("429 too many requests"),
		nil, // second attempt succeeds
	}}
	m := newResilientModel(primary, nil, "test", fastConfig(3), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err != nil {
		t.Fatalf("GenerateContent: %v", err)
	}
	if primary.callCount() != 2 {
		t.Errorf("called %d times, want 2", primary.callCount())
	}
}

func TestNonRetryableErrorFailsImmediately(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("invalid_api_key: the credential was rejected"),
		nil,
	}}
	m := newResilientModel(primary, nil, "test", fastConfig(3), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err == nil {
		t.Fatal("a rejected credential should fail rather than retry into success")
	}
	// A wrong key fails identically however many times it is sent. Retrying
	// costs the user several timeouts for no chance of a different answer.
	if primary.callCount() != 1 {
		t.Errorf("called %d times, want 1: a non-retryable error must not retry",
			primary.callCount())
	}
}

func TestRetryBudgetIsBounded(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("timeout"), errors.New("timeout"),
		errors.New("timeout"), errors.New("timeout"),
	}}
	m := newResilientModel(primary, nil, "test", fastConfig(2), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err == nil {
		t.Fatal("expected failure after the budget is spent")
	}
	// Unbounded retry turns one user's failure into sustained load on a
	// provider that is already struggling.
	if primary.callCount() != 2 {
		t.Errorf("called %d times, want exactly the 2-attempt budget",
			primary.callCount())
	}
}

func TestFallbackTakesOverWhenPrimaryIsExhausted(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("503 unavailable"), errors.New("503 unavailable"),
	}}
	fallback := &fakeModel{}
	m := newResilientModel(primary, fallback, "test", fastConfig(2), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err != nil {
		t.Fatalf("the fallback should have answered: %v", err)
	}
	if fallback.callCount() != 1 {
		t.Errorf("fallback called %d times, want 1", fallback.callCount())
	}
}

func TestFallbackIsNotRetried(t *testing.T) {
	primary := &fakeModel{errs: []error{errors.New("timeout")}}
	fallback := &fakeModel{errs: []error{
		errors.New("timeout"), errors.New("timeout"),
	}}
	m := newResilientModel(primary, fallback, "test", fastConfig(1), quietLogger())

	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err == nil {
		t.Fatal("expected failure when both fail")
	}
	// The fallback is already the degraded path; retrying it would double the
	// latency of a request that is by now the slowest in the system.
	if fallback.callCount() != 1 {
		t.Errorf("fallback called %d times, want 1", fallback.callCount())
	}
}

func TestCancellationIsNotAModelFailure(t *testing.T) {
	primary := &fakeModel{}
	cfg := fastConfig(3)
	cfg.BreakerThreshold = 1
	cfg.BreakerCooldown = time.Minute
	m := newResilientModel(primary, nil, "test", cfg, quietLogger()).(*resilientModel)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := m.GenerateContent(ctx, &model.Request{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	// A burst of user cancellations must not open the circuit on a provider
	// that is perfectly healthy.
	if m.breaker.failures() != 0 {
		t.Errorf("breaker recorded %d failures for a cancellation, want 0",
			m.breaker.failures())
	}
	if primary.callCount() != 0 {
		t.Errorf("primary called %d times for an already-cancelled context, want 0",
			primary.callCount())
	}
}

func TestBreakerOpensAndFailsFast(t *testing.T) {
	primary := &fakeModel{}
	for i := 0; i < 10; i++ {
		primary.errs = append(primary.errs, errors.New("503 unavailable"))
	}
	cfg := ResilienceConfig{
		MaxAttempts: 1, Backoff: time.Millisecond,
		BreakerThreshold: 3, BreakerCooldown: time.Minute,
	}
	m := newResilientModel(primary, nil, "test", cfg, quietLogger()).(*resilientModel)

	for i := 0; i < 3; i++ {
		if _, err := m.GenerateContent(context.Background(), &model.Request{}); err == nil {
			t.Fatalf("call %d should have failed", i)
		}
	}
	if !m.breaker.open() {
		t.Fatal("the circuit should be open after reaching the threshold")
	}

	before := primary.callCount()
	if _, err := m.GenerateContent(context.Background(), &model.Request{}); err == nil {
		t.Fatal("an open circuit should fail fast")
	}
	// Failing without paying for the call is the entire point: a dead
	// provider with a long timeout would otherwise hold every Worker slot.
	if primary.callCount() != before {
		t.Errorf("the primary was called while the circuit was open (%d → %d)",
			before, primary.callCount())
	}
}

func TestOpenCircuitUsesTheFallback(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("503"), errors.New("503"), errors.New("503"),
	}}
	fallback := &fakeModel{}
	cfg := ResilienceConfig{
		MaxAttempts: 1, Backoff: time.Millisecond,
		BreakerThreshold: 2, BreakerCooldown: time.Minute,
	}
	m := newResilientModel(primary, fallback, "test", cfg, quietLogger())

	// Drive the circuit open. The fallback answers throughout, so these
	// succeed — the user is served while the primary is skipped.
	for i := 0; i < 4; i++ {
		if _, err := m.GenerateContent(context.Background(), &model.Request{}); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	if fallback.callCount() < 3 {
		t.Errorf("fallback answered %d times, want it carrying the load once the circuit opened",
			fallback.callCount())
	}
}

func TestBreakerAllowsOneProbeAfterCooldown(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("503"), errors.New("503"),
	}}
	cfg := ResilienceConfig{
		MaxAttempts: 1, Backoff: time.Millisecond,
		BreakerThreshold: 2, BreakerCooldown: 20 * time.Millisecond,
	}
	m := newResilientModel(primary, nil, "test", cfg, quietLogger()).(*resilientModel)

	for i := 0; i < 2; i++ {
		m.GenerateContent(context.Background(), &model.Request{})
	}
	if !m.breaker.open() {
		t.Fatal("circuit should be open")
	}

	time.Sleep(30 * time.Millisecond)

	// One probe, not a flood. Releasing all queued traffic at once would hit
	// a recovering provider with exactly the burst that broke it.
	before := primary.callCount()
	m.GenerateContent(context.Background(), &model.Request{})
	if primary.callCount() != before+1 {
		t.Errorf("expected exactly one probe, calls went %d → %d",
			before, primary.callCount())
	}
}

func TestSuccessResetsTheBreaker(t *testing.T) {
	primary := &fakeModel{errs: []error{
		errors.New("503"), errors.New("503"), nil,
	}}
	cfg := ResilienceConfig{
		MaxAttempts: 1, Backoff: time.Millisecond,
		BreakerThreshold: 5, BreakerCooldown: time.Minute,
	}
	m := newResilientModel(primary, nil, "test", cfg, quietLogger()).(*resilientModel)

	m.GenerateContent(context.Background(), &model.Request{})
	m.GenerateContent(context.Background(), &model.Request{})
	if m.breaker.failures() != 2 {
		t.Fatalf("failures = %d, want 2", m.breaker.failures())
	}

	m.GenerateContent(context.Background(), &model.Request{})
	// A full reset rather than a gradual decay: decaying would keep the
	// circuit half-open long after the provider recovered.
	if m.breaker.failures() != 0 {
		t.Errorf("failures = %d after a success, want a full reset", m.breaker.failures())
	}
}

func TestRetryableClassification(t *testing.T) {
	retryable := []string{
		"timeout waiting for response", "context deadline exceeded upstream",
		"connection refused", "connection reset by peer", "unexpected EOF",
		"429 Too Many Requests", "rate limit exceeded",
		"503 Service Unavailable", "502 bad gateway", "server overloaded",
	}
	for _, msg := range retryable {
		if !retryableModelError(errors.New(msg)) {
			t.Errorf("%q should be retryable", msg)
		}
	}

	notRetryable := []string{
		"invalid_api_key", "401 Unauthorized", "403 Forbidden",
		"invalid_request: bad parameter", "404 not found",
		"context_length_exceeded", "content_filter triggered",
	}
	for _, msg := range notRetryable {
		if retryableModelError(errors.New(msg)) {
			t.Errorf("%q should not be retryable", msg)
		}
	}

	// Cancellation and deadline are the caller's decision, never retryable.
	if retryableModelError(context.Canceled) {
		t.Error("context.Canceled must not be retryable")
	}
	if retryableModelError(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded must not be retryable")
	}

	// An unrecognised error defaults to *not* retrying. Matching on message
	// text is fragile — a provider rewording its errors would silently change
	// the classification — so the default errs toward failing fast rather
	// than retrying something that was never going to help.
	if retryableModelError(errors.New("something entirely unfamiliar")) {
		t.Error("an unrecognised error should default to not retryable")
	}
	if retryableModelError(nil) {
		t.Error("nil must not be retryable")
	}
}

func TestResilienceFromParams(t *testing.T) {
	cfg := resilienceFromParams(map[string]any{
		"max_attempts":      float64(4),
		"retry_backoff":     "2s",
		"breaker_threshold": float64(10),
		"breaker_cooldown":  "1m",
	})
	if cfg.MaxAttempts != 4 {
		t.Errorf("MaxAttempts = %d, want 4", cfg.MaxAttempts)
	}
	if cfg.Backoff != 2*time.Second {
		t.Errorf("Backoff = %v, want 2s", cfg.Backoff)
	}
	if cfg.BreakerThreshold != 10 {
		t.Errorf("BreakerThreshold = %d, want 10", cfg.BreakerThreshold)
	}
	if cfg.BreakerCooldown != time.Minute {
		t.Errorf("BreakerCooldown = %v, want 1m", cfg.BreakerCooldown)
	}

	// Unset params fall back to defaults, and the defaults must be usable:
	// a version that configures nothing still gets retry and a breaker.
	def := resilienceFromParams(nil)
	if def.MaxAttempts < 2 || def.BreakerThreshold < 1 {
		t.Errorf("defaults are not usable: %+v", def)
	}

	// Garbage is ignored rather than rejected, so a version carrying a
	// parameter this build does not understand still assembles.
	junk := resilienceFromParams(map[string]any{
		"max_attempts":  "not a number",
		"retry_backoff": "not a duration",
	})
	if junk.MaxAttempts != DefaultResilience().MaxAttempts {
		t.Errorf("malformed params should fall back to the default, got %+v", junk)
	}
}

func TestBreakerDisabledWhenThresholdIsZero(t *testing.T) {
	primary := &fakeModel{}
	for i := 0; i < 20; i++ {
		primary.errs = append(primary.errs, errors.New("503"))
	}
	cfg := ResilienceConfig{MaxAttempts: 1, BreakerThreshold: 0}
	m := newResilientModel(primary, nil, "test", cfg, quietLogger()).(*resilientModel)

	for i := 0; i < 10; i++ {
		m.GenerateContent(context.Background(), &model.Request{})
	}
	// Zero disables the feature, so every call still reaches the provider.
	if primary.callCount() != 10 {
		t.Errorf("called %d times, want 10 with the breaker disabled",
			primary.callCount())
	}
	if m.breaker.open() {
		t.Error("a disabled breaker must never report open")
	}
}

func TestFallbackModelName(t *testing.T) {
	if got := fallbackModelName(map[string]any{"fallback_model": " gpt-4o-mini "}); got != "gpt-4o-mini" {
		t.Errorf("fallbackModelName = %q, want it trimmed", got)
	}
	if got := fallbackModelName(nil); got != "" {
		t.Errorf("fallbackModelName(nil) = %q, want empty", got)
	}
}
