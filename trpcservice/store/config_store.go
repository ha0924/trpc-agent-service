// 设计依据：docs/多租户与节点部署设计.md §2「租户资源模型」、§6.1「装配由配置驱动」

package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// ChannelBindingByWebhook resolves an inbound callback path to its binding.
//
// This is where a request acquires its tenant identity. The payload is
// untrusted and can claim anything, so the tenant must come from the binding
// the platform itself configured, keyed by a path only the platform issued.
func (s *Store) ChannelBindingByWebhook(ctx context.Context, path string) (*types.ChannelBinding, error) {
	const q = `
SELECT channel_binding_id, tenant_id, agent_app_id, env, channel,
       COALESCE(external_app_id, ''), COALESCE(webhook_path, ''),
       COALESCE(secret_ref, ''), capabilities, status
  FROM channel_bindings
 WHERE webhook_path = ? AND status = 'active'`

	var (
		b       types.ChannelBinding
		capsRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, path).Scan(
		&b.ChannelBindingID, &b.TenantID, &b.AgentAppID, &b.Env, &b.Channel,
		&b.ExternalAppID, &b.WebhookPath, &b.SecretRef, &capsRaw, &b.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("channel binding for %q: %w", path, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query channel binding %q: %w", path, err)
	}
	if err := decodeJSON(capsRaw, &b.Capabilities); err != nil {
		return nil, fmt.Errorf("decode capabilities for %s: %w", b.ChannelBindingID, err)
	}
	return &b, nil
}

// ChannelBindingByID loads a binding by its business identifier.
//
// Worker needs this on the outbound path: it knows which binding a session
// belongs to but not the webhook path, and it needs the binding's credentials
// and capability limits to deliver a reply.
func (s *Store) ChannelBindingByID(ctx context.Context, tenantID, bindingID string) (*types.ChannelBinding, error) {
	const q = `
SELECT channel_binding_id, tenant_id, agent_app_id, env, channel,
       COALESCE(external_app_id, ''), COALESCE(webhook_path, ''),
       COALESCE(secret_ref, ''), capabilities, status
  FROM channel_bindings
 WHERE tenant_id = ? AND channel_binding_id = ?`

	var (
		b       types.ChannelBinding
		capsRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, tenantID, bindingID).Scan(
		&b.ChannelBindingID, &b.TenantID, &b.AgentAppID, &b.Env, &b.Channel,
		&b.ExternalAppID, &b.WebhookPath, &b.SecretRef, &capsRaw, &b.Status,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("channel binding %q: %w", bindingID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query channel binding %q: %w", bindingID, err)
	}
	if err := decodeJSON(capsRaw, &b.Capabilities); err != nil {
		return nil, fmt.Errorf("decode capabilities for %s: %w", bindingID, err)
	}
	return &b, nil
}

// TenantByID loads a tenant.
func (s *Store) TenantByID(ctx context.Context, tenantID string) (*types.Tenant, error) {
	const q = `SELECT tenant_id, name, status, settings FROM tenants WHERE tenant_id = ?`

	var (
		t           types.Tenant
		settingsRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, tenantID).Scan(&t.TenantID, &t.Name, &t.Status, &settingsRaw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("tenant %q: %w", tenantID, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query tenant %q: %w", tenantID, err)
	}
	if err := decodeJSON(settingsRaw, &t.Settings); err != nil {
		return nil, fmt.Errorf("decode settings for tenant %q: %w", tenantID, err)
	}
	return &t, nil
}

// Deployment loads the release configuration for one environment.
func (s *Store) Deployment(ctx context.Context, tenantID, agentAppID, env string) (*types.Deployment, error) {
	const q = `
SELECT tenant_id, agent_app_id, env, routes, strategy
  FROM agent_deployments
 WHERE tenant_id = ? AND agent_app_id = ? AND env = ?`

	var (
		d           types.Deployment
		routesRaw   []byte
		strategyRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, tenantID, agentAppID, env).Scan(
		&d.TenantID, &d.AgentAppID, &d.Env, &routesRaw, &strategyRaw,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("deployment %s/%s/%s: %w", tenantID, agentAppID, env, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query deployment %s/%s/%s: %w", tenantID, agentAppID, env, err)
	}
	if err := decodeJSON(routesRaw, &d.Routes); err != nil {
		return nil, fmt.Errorf("decode routes for %s/%s/%s: %w", tenantID, agentAppID, env, err)
	}
	if err := decodeJSON(strategyRaw, &d.Strategy); err != nil {
		return nil, fmt.Errorf("decode strategy for %s/%s/%s: %w", tenantID, agentAppID, env, err)
	}
	return &d, nil
}

// AgentVersion loads one version's prompt and model configuration.
func (s *Store) AgentVersion(ctx context.Context, key types.RuntimeKey) (*types.AgentVersion, error) {
	const q = `
SELECT tenant_id, agent_app_id, version, status,
       COALESCE(system_prompt, ''), model_name,
       COALESCE(model_api_key_ref, ''), model_params, published_at
  FROM agent_versions
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ?`

	var (
		v         types.AgentVersion
		paramsRaw []byte
	)
	err := s.db.QueryRowContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion).Scan(
		&v.TenantID, &v.AgentAppID, &v.Version, &v.Status,
		&v.SystemPrompt, &v.ModelName, &v.ModelAPIKeyRef, &paramsRaw, &v.PublishedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("agent version %s: %w", key, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query agent version %s: %w", key, err)
	}
	if err := decodeJSON(paramsRaw, &v.ModelParams); err != nil {
		return nil, fmt.Errorf("decode model params for %s: %w", key, err)
	}
	return &v, nil
}

// RuntimeSpec gathers everything needed to assemble a Runtime: the version row
// plus the four binding tables.
//
// The five reads share one key, so they could be a single join. They are kept
// separate for clarity because the cost lands only on first assembly: a
// published version is immutable, so the assembled Runtime is cached and never
// invalidated.
func (s *Store) RuntimeSpec(ctx context.Context, key types.RuntimeKey) (*types.RuntimeSpec, error) {
	if !key.Valid() {
		return nil, fmt.Errorf("incomplete runtime key %s", key)
	}

	version, err := s.AgentVersion(ctx, key)
	if err != nil {
		return nil, err
	}
	// An unpublished version must never be assembled: drafts may reference
	// tools or models that were never reviewed.
	if !version.Published() {
		return nil, fmt.Errorf("agent version %s is %s, not published", key, version.Status)
	}

	spec := &types.RuntimeSpec{
		Key:            key,
		SystemPrompt:   version.SystemPrompt,
		ModelName:      version.ModelName,
		ModelAPIKeyRef: version.ModelAPIKeyRef,
		ModelParams:    version.ModelParams,
	}

	if spec.Tools, err = s.toolBindings(ctx, key); err != nil {
		return nil, err
	}
	if spec.Extensions, err = s.extensionBindings(ctx, key); err != nil {
		return nil, err
	}
	if spec.MCPServers, err = s.mcpBindings(ctx, key); err != nil {
		return nil, err
	}
	if spec.Skills, err = s.skillBindings(ctx, key); err != nil {
		return nil, err
	}
	return spec, nil
}

func (s *Store) toolBindings(ctx context.Context, key types.RuntimeKey) ([]types.ToolBinding, error) {
	const q = `
SELECT tool_name, mode, params
  FROM agent_tool_bindings
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ?
 ORDER BY tool_name`

	rows, err := s.db.QueryContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return nil, fmt.Errorf("query tool bindings for %s: %w", key, err)
	}
	defer rows.Close()

	var out []types.ToolBinding
	for rows.Next() {
		var (
			b         types.ToolBinding
			paramsRaw []byte
		)
		if err := rows.Scan(&b.ToolName, &b.Mode, &paramsRaw); err != nil {
			return nil, fmt.Errorf("scan tool binding for %s: %w", key, err)
		}
		if err := decodeJSON(paramsRaw, &b.Params); err != nil {
			return nil, fmt.Errorf("decode tool params for %s/%s: %w", key, b.ToolName, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) extensionBindings(ctx context.Context, key types.RuntimeKey) ([]types.ExtensionBinding, error) {
	// Ordering by priority is part of the contract, not a convenience:
	// redaction has to run before audit logging, so mount order is configured
	// rather than left to insertion order.
	const q = `
SELECT kind, extension_name, enabled, priority, params
  FROM agent_extension_bindings
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND enabled = 1
 ORDER BY kind, priority, extension_name`

	rows, err := s.db.QueryContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return nil, fmt.Errorf("query extension bindings for %s: %w", key, err)
	}
	defer rows.Close()

	var out []types.ExtensionBinding
	for rows.Next() {
		var (
			b         types.ExtensionBinding
			paramsRaw []byte
		)
		if err := rows.Scan(&b.Kind, &b.ExtensionName, &b.Enabled, &b.Priority, &paramsRaw); err != nil {
			return nil, fmt.Errorf("scan extension binding for %s: %w", key, err)
		}
		if err := decodeJSON(paramsRaw, &b.Params); err != nil {
			return nil, fmt.Errorf("decode extension params for %s/%s: %w", key, b.ExtensionName, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) mcpBindings(ctx context.Context, key types.RuntimeKey) ([]types.MCPBinding, error) {
	const q = `
SELECT server_name, enabled, tool_filter
  FROM agent_mcp_bindings
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND enabled = 1
 ORDER BY server_name`

	rows, err := s.db.QueryContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return nil, fmt.Errorf("query mcp bindings for %s: %w", key, err)
	}
	defer rows.Close()

	var out []types.MCPBinding
	for rows.Next() {
		var (
			b         types.MCPBinding
			filterRaw []byte
		)
		if err := rows.Scan(&b.ServerName, &b.Enabled, &filterRaw); err != nil {
			return nil, fmt.Errorf("scan mcp binding for %s: %w", key, err)
		}
		if err := decodeJSON(filterRaw, &b.ToolFilter); err != nil {
			return nil, fmt.Errorf("decode tool filter for %s/%s: %w", key, b.ServerName, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

func (s *Store) skillBindings(ctx context.Context, key types.RuntimeKey) ([]types.SkillBinding, error) {
	const q = `
SELECT skill_name, enabled, params
  FROM agent_skill_bindings
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND enabled = 1
 ORDER BY skill_name`

	rows, err := s.db.QueryContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return nil, fmt.Errorf("query skill bindings for %s: %w", key, err)
	}
	defer rows.Close()

	var out []types.SkillBinding
	for rows.Next() {
		var (
			b         types.SkillBinding
			paramsRaw []byte
		)
		if err := rows.Scan(&b.SkillName, &b.Enabled, &paramsRaw); err != nil {
			return nil, fmt.Errorf("scan skill binding for %s: %w", key, err)
		}
		if err := decodeJSON(paramsRaw, &b.Params); err != nil {
			return nil, fmt.Errorf("decode skill params for %s/%s: %w", key, b.SkillName, err)
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
