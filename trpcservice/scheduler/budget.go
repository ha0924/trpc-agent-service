// 设计依据：docs/数据模型设计.md §1.7「不落表的两项」预算计数器
//                docs/治理监控与安全设计.md 预算限制

package scheduler

import (
	"context"
	"fmt"
	"time"
)

// Budget counter keys live under their own namespace so they can be inspected
// and expired independently of leases and mailboxes.
const prefixBudget = "agent:budget:"

// BudgetPeriod is the window a counter accumulates over.
type BudgetPeriod string

const (
	// BudgetDaily resets at midnight local time.
	BudgetDaily BudgetPeriod = "daily"
	// BudgetMonthly resets on the first of the month.
	BudgetMonthly BudgetPeriod = "monthly"
)

// budgetKey builds the counter key for a tenant and period.
//
// The period boundary is baked into the key rather than managed by a reset
// job: a new day simply uses a new key, so there is no moment where a reset
// races with a concurrent increment, and an old key expires on its own.
func budgetKey(tenantID string, period BudgetPeriod, now time.Time) string {
	switch period {
	case BudgetMonthly:
		return fmt.Sprintf("%s%s:monthly:%s", prefixBudget, tenantID, now.Format("2006-01"))
	default:
		return fmt.Sprintf("%s%s:daily:%s", prefixBudget, tenantID, now.Format("2006-01-02"))
	}
}

// budgetTTL keeps a counter slightly past its window so late-arriving usage
// still lands on the right key, then lets Redis reclaim it.
func budgetTTL(period BudgetPeriod) time.Duration {
	if period == BudgetMonthly {
		return 35 * 24 * time.Hour
	}
	return 48 * time.Hour
}

// AddTokens increments a tenant's usage counter and returns the new total.
//
// Usage lives in Redis rather than being summed from usage_records because a
// budget check happens before every model call: summing a growing detail table
// on the hot path is too slow, and several Workers incrementing concurrently
// need an atomic operation. usage_records remains the ledger for
// reconciliation.
func (r *Redis) AddTokens(ctx context.Context, tenantID string, period BudgetPeriod, tokens int64) (int64, error) {
	key := budgetKey(tenantID, period, time.Now())

	total, err := r.client.IncrBy(ctx, key, tokens).Result()
	if err != nil {
		return 0, fmt.Errorf("increment budget for %s: %w", tenantID, err)
	}
	// Set the expiry only on first write. Refreshing it on every increment
	// would keep a busy tenant's counter alive indefinitely.
	if total == tokens {
		if err := r.client.Expire(ctx, key, budgetTTL(period)).Err(); err != nil {
			return total, fmt.Errorf("set budget ttl for %s: %w", tenantID, err)
		}
	}
	return total, nil
}

// UsedTokens reads a tenant's current usage without changing it.
func (r *Redis) UsedTokens(ctx context.Context, tenantID string, period BudgetPeriod) (int64, error) {
	key := budgetKey(tenantID, period, time.Now())

	n, err := r.client.Get(ctx, key).Int64()
	if err != nil {
		if isRedisNil(err) {
			return 0, nil // no usage yet in this window
		}
		return 0, fmt.Errorf("read budget for %s: %w", tenantID, err)
	}
	return n, nil
}

// AllowRate reports whether an action is within a per-minute rate limit.
//
// The window is the current minute, encoded in the key for the same reason as
// budgets: a new minute is a new key, so there is no reset to race with.
func (r *Redis) AllowRate(ctx context.Context, scope string, limitPerMin int) (bool, error) {
	if limitPerMin <= 0 {
		return true, nil
	}
	key := fmt.Sprintf("%srate:%s:%s", prefixBudget, scope, time.Now().Format("2006-01-02T15:04"))

	n, err := r.client.Incr(ctx, key).Result()
	if err != nil {
		return false, fmt.Errorf("increment rate counter for %s: %w", scope, err)
	}
	if n == 1 {
		if err := r.client.Expire(ctx, key, 2*time.Minute).Err(); err != nil {
			return false, fmt.Errorf("set rate ttl for %s: %w", scope, err)
		}
	}
	return n <= int64(limitPerMin), nil
}

// ResetBudget clears a tenant's counter, for administrative correction.
func (r *Redis) ResetBudget(ctx context.Context, tenantID string, period BudgetPeriod) error {
	key := budgetKey(tenantID, period, time.Now())
	if err := r.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("reset budget for %s: %w", tenantID, err)
	}
	return nil
}
