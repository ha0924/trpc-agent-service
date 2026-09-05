// 设计依据：docs/IM通道接入设计.md §7.2「② 加密通道…③ access_token 的缓存与并发」
//                docs/风险清单.md #9「出站凭据并发刷新互相顶掉」

package wecom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/redis/go-redis/v9"
)

// Token cache keys live under their own namespace, scoped per binding: two
// bindings of the same tenant may use different applications and therefore
// different credentials.
const tokenKeyPrefix = "agent:wecom:token:"

// tokenLockPrefix guards concurrent refresh.
const tokenLockPrefix = "agent:wecom:token:lock:"

// TokenManager fetches and caches WeCom access tokens.
//
// WeCom issues a token valid for roughly two hours, and — critically —
// requesting a new one may invalidate the previous one. So several Workers
// refreshing at the same moment do not merely waste calls: the token the
// first Worker just cached can be killed by the second, and deliveries fail
// intermittently in a way that is hard to trace.
//
// Two measures prevent that. Refresh happens under a distributed lock, so one
// process fetches and the rest reuse the result. And refresh is early — well
// before expiry — so no request ever waits on a token that has already
// stopped working.
type TokenManager struct {
	client *redis.Client
	http   *http.Client
	log    *slog.Logger

	// refreshMargin is how long before expiry a token is treated as stale.
	refreshMargin time.Duration
}

// NewTokenManager builds a manager backed by Redis.
func NewTokenManager(client *redis.Client, logger *slog.Logger) *TokenManager {
	if logger == nil {
		logger = slog.Default()
	}
	return &TokenManager{
		client:        client,
		http:          &http.Client{Timeout: 10 * time.Second},
		log:           logger,
		refreshMargin: 20 * time.Minute,
	}
}

// Credentials are the per-binding values needed to obtain a token.
type Credentials struct {
	CorpID string `json:"corp_id"`
	// Secret is the application secret. It is read from the secret manager
	// per call and never stored on a struct that outlives the call.
	Secret  string `json:"secret"`
	AgentID int64  `json:"agent_id"`
	// Token and EncodingAESKey belong to the callback, not the token API, but
	// travel together as one credential blob.
	Token          string `json:"token"`
	EncodingAESKey string `json:"encoding_aes_key"`
}

// Valid reports whether the credential blob is usable.
func (c *Credentials) Valid() error {
	switch {
	case c == nil:
		return fmt.Errorf("wecom: nil credentials")
	case c.CorpID == "":
		return fmt.Errorf("wecom: corp_id is required")
	case c.Secret == "":
		return fmt.Errorf("wecom: secret is required")
	case c.AgentID == 0:
		return fmt.Errorf("wecom: agent_id is required")
	}
	return nil
}

// ParseCredentials decodes the JSON blob stored behind a secret reference.
func ParseCredentials(raw string) (*Credentials, error) {
	var c Credentials
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		return nil, fmt.Errorf("wecom: parse credentials: %w", err)
	}
	return &c, nil
}

// tokenResponse is what the gettoken endpoint returns.
type tokenResponse struct {
	ErrCode     int    `json:"errcode"`
	ErrMsg      string `json:"errmsg"`
	AccessToken string `json:"access_token"`
	ExpiresIn   int    `json:"expires_in"`
}

// Token returns a usable access token for the binding, refreshing if needed.
func (m *TokenManager) Token(ctx context.Context, bindingID string, creds *Credentials) (string, error) {
	if err := creds.Valid(); err != nil {
		return "", err
	}
	key := tokenKeyPrefix + bindingID

	if tok, err := m.client.Get(ctx, key).Result(); err == nil && tok != "" {
		return tok, nil
	} else if err != nil && err != redis.Nil {
		// Redis being unavailable must not silently mean "fetch a new token
		// every call": that is exactly the pattern that invalidates the token
		// other Workers are holding. Fall through, but say so.
		m.log.Warn("wecom token cache unavailable, refreshing",
			"binding_id", bindingID, "error", err.Error())
	}

	return m.refresh(ctx, bindingID, creds)
}

// refresh fetches a new token under a lock.
func (m *TokenManager) refresh(ctx context.Context, bindingID string, creds *Credentials) (string, error) {
	lockKey := tokenLockPrefix + bindingID
	lockVal := fmt.Sprintf("%d", time.Now().UnixNano())

	got, err := m.client.SetNX(ctx, lockKey, lockVal, 15*time.Second).Result()
	if err != nil {
		m.log.Warn("wecom token lock unavailable", "binding_id", bindingID, "error", err.Error())
		got = true // proceed unlocked rather than fail delivery outright
	}

	if !got {
		// Another process is refreshing. Wait briefly and read its result
		// rather than fetching in parallel, which could invalidate the token
		// it is about to publish.
		if tok, ok := m.waitForRefresh(ctx, bindingID); ok {
			return tok, nil
		}
		m.log.Warn("wecom token refresh wait timed out, fetching anyway", "binding_id", bindingID)
	}
	defer m.client.Del(context.WithoutCancel(ctx), lockKey)

	// Re-read after taking the lock: another process may have finished
	// between our cache miss and our lock acquisition.
	if tok, err := m.client.Get(ctx, tokenKeyPrefix+bindingID).Result(); err == nil && tok != "" {
		return tok, nil
	}

	tok, ttl, err := m.fetch(ctx, creds)
	if err != nil {
		return "", err
	}

	// Cached for less than its real lifetime, so a token is replaced before
	// it can expire mid-delivery.
	cacheFor := ttl - m.refreshMargin
	if cacheFor < time.Minute {
		cacheFor = time.Minute
	}
	if err := m.client.Set(ctx, tokenKeyPrefix+bindingID, tok, cacheFor).Err(); err != nil {
		m.log.Warn("caching wecom token failed", "binding_id", bindingID, "error", err.Error())
	}
	m.log.Info("wecom token refreshed", "binding_id", bindingID, "cached_for", cacheFor.String())
	return tok, nil
}

// waitForRefresh polls briefly for another process's token.
func (m *TokenManager) waitForRefresh(ctx context.Context, bindingID string) (string, bool) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(100 * time.Millisecond):
		}
		if tok, err := m.client.Get(ctx, tokenKeyPrefix+bindingID).Result(); err == nil && tok != "" {
			return tok, true
		}
	}
	return "", false
}

// fetch calls the gettoken endpoint.
func (m *TokenManager) fetch(ctx context.Context, creds *Credentials) (string, time.Duration, error) {
	endpoint := "https://qyapi.weixin.qq.com/cgi-bin/gettoken?" + url.Values{
		"corpid":     {creds.CorpID},
		"corpsecret": {creds.Secret},
	}.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", 0, fmt.Errorf("wecom: build token request: %w", err)
	}

	resp, err := m.http.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("wecom: fetch token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return "", 0, fmt.Errorf("wecom: read token response: %w", err)
	}

	var out tokenResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return "", 0, fmt.Errorf("wecom: parse token response: %w", err)
	}
	if out.ErrCode != 0 {
		// The corp id and secret are deliberately absent from this message:
		// it will be logged, and a secret in a log is the failure this whole
		// design exists to avoid.
		return "", 0, fmt.Errorf("wecom: gettoken failed: errcode=%d errmsg=%s", out.ErrCode, out.ErrMsg)
	}
	if out.AccessToken == "" {
		return "", 0, fmt.Errorf("wecom: gettoken returned an empty token")
	}

	ttl := time.Duration(out.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 2 * time.Hour
	}
	return out.AccessToken, ttl, nil
}

// Invalidate drops a cached token, for use when the API reports it expired.
//
// WeCom can reject a token that has not yet reached its cached expiry — for
// instance because another system refreshed it. Dropping and retrying once is
// the correct response; retrying with the same token is not.
func (m *TokenManager) Invalidate(ctx context.Context, bindingID string) {
	if err := m.client.Del(ctx, tokenKeyPrefix+bindingID).Err(); err != nil {
		m.log.Warn("invalidating wecom token failed", "binding_id", bindingID, "error", err.Error())
	}
}
