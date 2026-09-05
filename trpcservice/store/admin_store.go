// 设计依据：docs/技术设计方案.md §4.2 Admin API
//                docs/故障恢复与运维设计.md 灰度发布和回滚

package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// ListTenants returns every tenant.
func (s *Store) ListTenants(ctx context.Context) ([]types.Tenant, error) {
	const q = `SELECT tenant_id, name, status, settings FROM tenants ORDER BY tenant_id`

	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("query tenants: %w", err)
	}
	defer rows.Close()

	var out []types.Tenant
	for rows.Next() {
		var (
			t   types.Tenant
			raw []byte
		)
		if err := rows.Scan(&t.TenantID, &t.Name, &t.Status, &raw); err != nil {
			return nil, fmt.Errorf("scan tenant: %w", err)
		}
		if err := decodeJSON(raw, &t.Settings); err != nil {
			return nil, fmt.Errorf("decode settings for %s: %w", t.TenantID, err)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// ListAgentApps returns a tenant's agents.
func (s *Store) ListAgentApps(ctx context.Context, tenantID string) ([]types.AgentApp, error) {
	const q = `
SELECT tenant_id, agent_app_id, name, COALESCE(description, ''), status
  FROM agent_apps WHERE tenant_id = ? ORDER BY agent_app_id`

	rows, err := s.db.QueryContext(ctx, q, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query agent apps for %s: %w", tenantID, err)
	}
	defer rows.Close()

	var out []types.AgentApp
	for rows.Next() {
		var a types.AgentApp
		if err := rows.Scan(&a.TenantID, &a.AgentAppID, &a.Name, &a.Description, &a.Status); err != nil {
			return nil, fmt.Errorf("scan agent app: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// ListAgentVersions returns an agent's versions, newest first.
func (s *Store) ListAgentVersions(ctx context.Context, tenantID, agentAppID string) ([]types.AgentVersion, error) {
	const q = `
SELECT tenant_id, agent_app_id, version, status,
       COALESCE(system_prompt, ''), model_name,
       COALESCE(model_api_key_ref, ''), model_params, published_at
  FROM agent_versions
 WHERE tenant_id = ? AND agent_app_id = ?
 ORDER BY id DESC`

	rows, err := s.db.QueryContext(ctx, q, tenantID, agentAppID)
	if err != nil {
		return nil, fmt.Errorf("query versions for %s/%s: %w", tenantID, agentAppID, err)
	}
	defer rows.Close()

	var out []types.AgentVersion
	for rows.Next() {
		var (
			v   types.AgentVersion
			raw []byte
		)
		if err := rows.Scan(&v.TenantID, &v.AgentAppID, &v.Version, &v.Status,
			&v.SystemPrompt, &v.ModelName, &v.ModelAPIKeyRef, &raw, &v.PublishedAt); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		if err := decodeJSON(raw, &v.ModelParams); err != nil {
			return nil, fmt.Errorf("decode model params for %s: %w", v.Version, err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// UpdateDeployment replaces an environment's routing weights.
//
// One statement on one row, so a rollout and a rollback are both atomic. The
// caller has already validated that the versions exist, are published, and
// that the weights sum to 100.
func (s *Store) UpdateDeployment(ctx context.Context, d *types.Deployment, updatedBy string) error {
	routes, err := json.Marshal(d.Routes)
	if err != nil {
		return fmt.Errorf("encode routes: %w", err)
	}

	const q = `
INSERT INTO agent_deployments (tenant_id, agent_app_id, env, routes, updated_by)
VALUES (?, ?, ?, ?, ?) AS new
ON DUPLICATE KEY UPDATE routes = new.routes, updated_by = new.updated_by`

	if _, err := s.db.ExecContext(ctx, q,
		d.TenantID, d.AgentAppID, d.Env, string(routes), nullString(updatedBy)); err != nil {
		return fmt.Errorf("update deployment %s/%s/%s: %w", d.TenantID, d.AgentAppID, d.Env, err)
	}
	return nil
}

// ListChannelBindings returns a tenant's channel bindings.
//
// secret_ref is included but is only a reference; the credential itself never
// leaves the secret manager.
func (s *Store) ListChannelBindings(ctx context.Context, tenantID string) ([]types.ChannelBinding, error) {
	const q = `
SELECT channel_binding_id, tenant_id, agent_app_id, env, channel,
       COALESCE(external_app_id, ''), COALESCE(webhook_path, ''),
       COALESCE(secret_ref, ''), capabilities, status
  FROM channel_bindings WHERE tenant_id = ? ORDER BY channel_binding_id`

	rows, err := s.db.QueryContext(ctx, q, tenantID)
	if err != nil {
		return nil, fmt.Errorf("query bindings for %s: %w", tenantID, err)
	}
	defer rows.Close()

	var out []types.ChannelBinding
	for rows.Next() {
		var (
			b   types.ChannelBinding
			raw []byte
		)
		if err := rows.Scan(&b.ChannelBindingID, &b.TenantID, &b.AgentAppID, &b.Env,
			&b.Channel, &b.ExternalAppID, &b.WebhookPath, &b.SecretRef, &raw, &b.Status); err != nil {
			return nil, fmt.Errorf("scan binding: %w", err)
		}
		if err := decodeJSON(raw, &b.Capabilities); err != nil {
			return nil, fmt.Errorf("decode capabilities for %s: %w", b.ChannelBindingID, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ListSessions returns a tenant's most recently active sessions.
func (s *Store) ListSessions(ctx context.Context, tenantID string, limit int) ([]types.Session, error) {
	const q = `
SELECT session_id, tenant_id, agent_app_id, agent_version, channel,
       channel_binding_id, scope, scope_key, COALESCE(internal_user_id, ''),
       last_sequence, status, last_active_at
  FROM sessions
 WHERE tenant_id = ?
 ORDER BY last_active_at DESC
 LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("query sessions for %s: %w", tenantID, err)
	}
	defer rows.Close()

	var out []types.Session
	for rows.Next() {
		var sess types.Session
		if err := rows.Scan(&sess.SessionID, &sess.TenantID, &sess.AgentAppID,
			&sess.AgentVersion, &sess.Channel, &sess.ChannelBindingID, &sess.Scope,
			&sess.ScopeKey, &sess.InternalUserID, &sess.LastSequence,
			&sess.Status, &sess.LastActiveAt); err != nil {
			return nil, fmt.Errorf("scan session: %w", err)
		}
		out = append(out, sess)
	}
	return out, rows.Err()
}

// AuditRow is an audit record as returned to the admin API.
type AuditRow struct {
	types.AuditRecord
	CreatedAt time.Time `json:"created_at"`
}

// ListAudit returns a tenant's most recent audit records.
func (s *Store) ListAudit(ctx context.Context, tenantID string, limit int) ([]AuditRow, error) {
	const q = `
SELECT tenant_id, COALESCE(agent_app_id,''), COALESCE(agent_name,''),
       COALESCE(channel,''), COALESCE(user_id,''), COALESCE(session_id,''),
       request_id, COALESCE(trace_id,''), event_type, COALESCE(tool_name,''),
       decision, COALESCE(reason,''), COALESCE(latency_ms,0),
       COALESCE(error_type,''), COALESCE(cost_usd,0), created_at
  FROM audit_logs
 WHERE tenant_id = ?
 ORDER BY id DESC
 LIMIT ?`

	rows, err := s.db.QueryContext(ctx, q, tenantID, limit)
	if err != nil {
		return nil, fmt.Errorf("query audit for %s: %w", tenantID, err)
	}
	defer rows.Close()

	var out []AuditRow
	for rows.Next() {
		var r AuditRow
		if err := rows.Scan(&r.TenantID, &r.AgentAppID, &r.AgentName, &r.Channel,
			&r.UserID, &r.SessionID, &r.RequestID, &r.TraceID, &r.EventType,
			&r.ToolName, &r.Decision, &r.Reason, &r.LatencyMS, &r.ErrorType,
			&r.CostUSD, &r.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan audit row: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// UsageSummary aggregates a tenant's spend over a window.
type UsageSummary struct {
	TenantID         string    `json:"tenant_id"`
	Since            time.Time `json:"since"`
	Requests         int64     `json:"requests"`
	PromptTokens     int64     `json:"prompt_tokens"`
	CompletionTokens int64     `json:"completion_tokens"`
	TotalTokens      int64     `json:"total_tokens"`
	CostUSD          float64   `json:"cost_usd"`
}

// UsageSummary reads the ledger for a reporting view.
//
// This is reconciliation, not enforcement. A budget check runs before every
// model call and reads the Redis counter instead: aggregating a growing detail
// table on the request path would be far too slow.
func (s *Store) UsageSummary(ctx context.Context, tenantID string, since time.Time) (*UsageSummary, error) {
	const q = `
SELECT COUNT(*), COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0),
       COALESCE(SUM(total_tokens),0), COALESCE(SUM(cost_usd),0)
  FROM usage_records
 WHERE tenant_id = ? AND created_at >= ?`

	out := &UsageSummary{TenantID: tenantID, Since: since}
	if err := s.db.QueryRowContext(ctx, q, tenantID, since).Scan(
		&out.Requests, &out.PromptTokens, &out.CompletionTokens,
		&out.TotalTokens, &out.CostUSD); err != nil {
		return nil, fmt.Errorf("summarise usage for %s: %w", tenantID, err)
	}
	return out, nil
}
