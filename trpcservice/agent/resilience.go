// 设计依据：docs/风险清单.md #6「模型超时、限流或配额耗尽」
//                docs/故障恢复与运维设计.md「模型降级」

package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"trpc.group/trpc-go/trpc-agent-go/model"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
)

// resilientModel adds bounded retry, a circuit breaker and a fallback to a
// model client.
//
// 风险清单 #6 lists four mitigations for "模型超时、限流或配额耗尽": timeout
// propagation, bounded retry with backoff, a fallback model, and a circuit
// breaker. Only the first was implemented — context cancellation already
// reached the provider — so a provider outage meant every request failed for
// as long as it lasted, and the retries of those failures made the throttling
// worse.
//
// The one property that shapes everything here: model.Model returns a
// *channel*, and retry is only safe before anything has been read from it.
//
// Once a response has been forwarded, the caller may already have appended a
// partial assistant message; re-running the call would produce a second,
// different continuation of the same turn. So this wrapper retries only
// failures that happen *before* the first response, and passes mid-stream
// failures through untouched. That is a real limitation, not an oversight,
// and it is why the retry budget is small: it covers connection refusals and
// immediate 429s, not a provider that dies halfway through generating.
type resilientModel struct {
	primary  model.Model
	fallback model.Model

	cfg  ResilienceConfig
	name string
	log  *slog.Logger

	breaker *breaker
}

// ResilienceConfig tunes the wrapper. Zero values disable each feature
// individually, so a version can opt into retry without a breaker.
type ResilienceConfig struct {
	// MaxAttempts counts the first try. 1 or 0 disables retry.
	//
	// Deliberately small. A model call is expensive and slow; retrying it
	// many times turns one user's failure into sustained load on a provider
	// that is already struggling — the behaviour that makes throttling
	// self-reinforcing.
	MaxAttempts int

	// Backoff is the wait before the second attempt, doubled thereafter.
	Backoff time.Duration

	// BreakerThreshold is the consecutive-failure count that opens the
	// circuit. 0 disables the breaker.
	BreakerThreshold int

	// BreakerCooldown is how long the circuit stays open before one probe is
	// allowed through.
	BreakerCooldown time.Duration
}

// DefaultResilience is applied when a version configures nothing.
//
// Two attempts rather than three: the second covers a transient connection
// failure, and beyond that the problem is usually not transient. Waiting
// longer would push the caller past its own timeout, converting a fast
// failure into a slow one.
func DefaultResilience() ResilienceConfig {
	return ResilienceConfig{
		MaxAttempts:      2,
		Backoff:          500 * time.Millisecond,
		BreakerThreshold: 5,
		BreakerCooldown:  30 * time.Second,
	}
}

// resilienceFromParams reads the knobs from a version's model_params.
//
// Per version rather than global: a tenant running a batch job may accept
// slower retries than one serving an IM conversation, and that is exactly the
// kind of difference the version snapshot exists to hold.
func resilienceFromParams(params map[string]any) ResilienceConfig {
	cfg := DefaultResilience()
	if v, ok := numeric(params["max_attempts"]); ok && v >= 1 {
		cfg.MaxAttempts = int(v)
	}
	if d, ok := params["retry_backoff"].(string); ok {
		if parsed, err := time.ParseDuration(d); err == nil {
			cfg.Backoff = parsed
		}
	}
	if v, ok := numeric(params["breaker_threshold"]); ok && v >= 0 {
		cfg.BreakerThreshold = int(v)
	}
	if d, ok := params["breaker_cooldown"].(string); ok {
		if parsed, err := time.ParseDuration(d); err == nil {
			cfg.BreakerCooldown = parsed
		}
	}
	return cfg
}

// newResilientModel wraps primary, optionally with a fallback.
func newResilientModel(
	primary, fallback model.Model,
	name string,
	cfg ResilienceConfig,
	logger *slog.Logger,
) model.Model {
	if logger == nil {
		logger = slog.Default()
	}
	return &resilientModel{
		primary:  primary,
		fallback: fallback,
		cfg:      cfg,
		name:     name,
		log:      logger,
		breaker:  newBreaker(cfg.BreakerThreshold, cfg.BreakerCooldown),
	}
}

// Info reports the primary's identity.
//
// The primary's, not the fallback's, even while the circuit is open: Info
// describes what this agent version is configured to use, and a caller
// reading it to build a prompt or a metric label must see a stable answer.
func (m *resilientModel) Info() model.Info { return m.primary.Info() }

// GenerateContent calls the model with retry, breaker and fallback.
func (m *resilientModel) GenerateContent(
	ctx context.Context,
	req *model.Request,
) (<-chan *model.Response, error) {
	// Breaker first: the point of an open circuit is to fail without paying
	// for the call.
	if !m.breaker.allow() {
		m.log.Warn("model circuit open, skipping the primary",
			"model", m.name, "consecutive_failures", m.breaker.failures())
		if m.fallback != nil {
			return m.callFallback(ctx, req, "circuit open")
		}
		return nil, fmt.Errorf("model %s: circuit open after %d consecutive failures",
			m.name, m.breaker.failures())
	}

	var lastErr error
	backoff := m.cfg.Backoff

	for attempt := 1; attempt <= max(1, m.cfg.MaxAttempts); attempt++ {
		if ctx.Err() != nil {
			// The caller gave up. Not a model failure, so it must not count
			// toward the breaker — otherwise a burst of user cancellations
			// would open the circuit on a healthy provider.
			return nil, ctx.Err()
		}

		ch, err := m.primary.GenerateContent(ctx, req)
		if err == nil {
			m.breaker.succeed()
			return ch, nil
		}
		lastErr = err

		if !retryableModelError(err) {
			// A malformed request or a rejected credential fails identically
			// however many times it is sent. Retrying wastes the user's time
			// and the provider's quota.
			m.breaker.fail()
			m.log.Warn("model call failed with a non-retryable error",
				"model", m.name, "attempt", attempt,
				"error", applog.Scrub(err.Error()))
			break
		}

		m.breaker.fail()
		if attempt < max(1, m.cfg.MaxAttempts) {
			m.log.Warn("model call failed, retrying",
				"model", m.name, "attempt", attempt,
				"backoff", backoff.String(),
				"error", applog.Scrub(err.Error()))
			if !sleepContext(ctx, backoff) {
				return nil, ctx.Err()
			}
			backoff *= 2
		}
	}

	if m.fallback != nil {
		m.log.Warn("primary model exhausted, using the fallback",
			"model", m.name, "error", applog.Scrub(errString(lastErr)))
		return m.callFallback(ctx, req, "primary exhausted")
	}
	return nil, fmt.Errorf("model %s: %w", m.name, lastErr)
}

// callFallback runs the fallback once.
//
// Once, with no retry and outside the breaker: the fallback is already the
// degraded path, and retrying it would double the latency of a request that
// is by now the slowest in the system. If it fails, the turn fails.
func (m *resilientModel) callFallback(
	ctx context.Context,
	req *model.Request,
	why string,
) (<-chan *model.Response, error) {
	ch, err := m.fallback.GenerateContent(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("model %s: %s and the fallback also failed: %w",
			m.name, why, err)
	}
	return ch, nil
}

// retryableModelError reports whether another attempt could plausibly differ.
//
// Matched on text because the framework's model clients wrap provider errors
// without a typed taxonomy. That is fragile and worth stating plainly: a
// provider that rewords its messages would silently turn a retryable failure
// into a non-retryable one.
//
// The default is therefore *not* to retry. An unrecognised error fails fast,
// which errs toward the user seeing a prompt failure rather than waiting
// through retries that were never going to help.
func retryableModelError(err error) bool {
	if err == nil {
		return false
	}
	// Cancellation and deadline are the caller's decision, never retryable.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	msg := strings.ToLower(err.Error())

	// Definitely not worth retrying: the request or the credential is wrong.
	for _, s := range []string{
		"invalid_api_key", "unauthorized", "401", "403",
		"invalid_request", "400", "not found", "404",
		"context_length_exceeded", "content_filter",
	} {
		if strings.Contains(msg, s) {
			return false
		}
	}

	// Worth one more try: transport failures and explicit backpressure.
	for _, s := range []string{
		"timeout", "deadline", "connection refused", "connection reset",
		"eof", "no such host", "temporary failure",
		"429", "rate limit", "too many requests",
		"500", "502", "503", "504", "overloaded", "unavailable",
	} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------

// breaker fails fast once a provider has failed repeatedly.
//
// Its purpose is not to protect the provider — that is a side effect — but to
// protect this platform. A dead provider with a 30-second timeout will hold
// every Worker slot for 30 seconds each; with the circuit open those requests
// fail immediately and the Workers stay free for tenants on other models.
type breaker struct {
	threshold int
	cooldown  time.Duration

	mu           sync.Mutex
	consecutive  int
	openedAt     time.Time
	probeAllowed bool
}

func newBreaker(threshold int, cooldown time.Duration) *breaker {
	return &breaker{threshold: threshold, cooldown: cooldown}
}

// allow reports whether a call may proceed.
//
// After the cooldown one probe is let through rather than reopening the gate
// fully. Releasing all queued traffic at once would hit a provider that may
// still be recovering with exactly the burst that broke it.
func (b *breaker) allow() bool {
	if b == nil || b.threshold <= 0 {
		return true // breaker disabled
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.consecutive < b.threshold {
		return true
	}
	if time.Since(b.openedAt) < b.cooldown {
		return false
	}
	if b.probeAllowed {
		// A probe is already in flight; everyone else keeps failing fast.
		return false
	}
	b.probeAllowed = true
	return true
}

func (b *breaker) succeed() {
	if b == nil || b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	// A success resets fully. Decaying gradually would keep the circuit
	// half-open long after the provider recovered.
	b.consecutive = 0
	b.probeAllowed = false
}

func (b *breaker) fail() {
	if b == nil || b.threshold <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.consecutive++
	b.probeAllowed = false
	if b.consecutive == b.threshold {
		b.openedAt = time.Now()
	} else if b.consecutive > b.threshold {
		// A failed probe restarts the cooldown, so a provider that is still
		// down is not probed on every subsequent request.
		b.openedAt = time.Now()
	}
}

func (b *breaker) failures() int {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutive
}

// open reports whether the circuit is currently open, for diagnostics.
func (b *breaker) open() bool {
	if b == nil || b.threshold <= 0 {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.consecutive >= b.threshold && time.Since(b.openedAt) < b.cooldown
}

func sleepContext(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// fallbackModelName reads the version's configured fallback.
//
// A name rather than a full second model configuration: the fallback shares
// the tenant's credential and base URL, and asking an operator to restate
// those invites them to drift apart.
func fallbackModelName(params map[string]any) string {
	s, _ := params["fallback_model"].(string)
	return strings.TrimSpace(s)
}

// compile-time proof the wrapper is substitutable for a model.
var _ model.Model = (*resilientModel)(nil)
