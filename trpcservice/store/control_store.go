// 设计依据：docs/多租户与节点部署设计.md §2「租户资源模型」
//                docs/技术设计方案.md §4.2 Admin API
//                docs/数据模型设计.md §5「核心表结构」

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// This file holds the control-plane writes: creating tenants, agents and
// versions, and configuring their capabilities.
//
// Until now the platform could only read its own configuration — every
// creation meant hand-written SQL. That contradicted the first line of the
// brief, "tenants create and deploy their own agents": deploying worked
// (weights are a single-row update) but creating did not exist.
//
// Two rules run through every function here:
//
//   - Tenancy is a parameter, never inferred. Each statement carries its
//     tenant_id in the WHERE or the INSERT, so a caller cannot reach another
//     tenant's row even by supplying its id.
//   - A published version is never mutated. The only status transitions are
//     draft→published and published→archived; editing a published version is
//     not offered because a cached Runtime keyed by version must never go
//     stale.

// CreateTenant inserts a tenant.
//
// Returns ErrDuplicate when the id is taken, so the caller can answer 409
// rather than reporting a generic failure — "this name is taken" and "the
// database is down" need different reactions from an operator.
func (s *Store) CreateTenant(ctx context.Context, t *types.Tenant) error {
	if t == nil {
		return errors.New("create tenant: nil tenant")
	}
	settings, err := json.Marshal(orEmptyMap(t.Settings))
	if err != nil {
		return fmt.Errorf("encode settings for %s: %w", t.TenantID, err)
	}

	const q = `INSERT INTO tenants (tenant_id, name, status, settings) VALUES (?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, q, t.TenantID, t.Name, t.Status, string(settings))
	if isDuplicate(err) {
		return fmt.Errorf("tenant %s: %w", t.TenantID, ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("create tenant %s: %w", t.TenantID, err)
	}
	return nil
}

// UpdateTenant replaces a tenant's mutable fields.
//
// Settings are replaced wholesale rather than merged. A merge would make it
// impossible to remove a budget: omitting a key would be indistinguishable
// from leaving it unchanged.
func (s *Store) UpdateTenant(ctx context.Context, t *types.Tenant) error {
	if t == nil {
		return errors.New("update tenant: nil tenant")
	}
	settings, err := json.Marshal(orEmptyMap(t.Settings))
	if err != nil {
		return fmt.Errorf("encode settings for %s: %w", t.TenantID, err)
	}

	const q = `UPDATE tenants SET name = ?, status = ?, settings = ? WHERE tenant_id = ?`

	res, err := s.db.ExecContext(ctx, q, t.Name, t.Status, string(settings), t.TenantID)
	if err != nil {
		return fmt.Errorf("update tenant %s: %w", t.TenantID, err)
	}
	return requireAffected(res, fmt.Sprintf("tenant %s", t.TenantID))
}

// SetTenantStatus suspends or reactivates a tenant.
//
// Separate from UpdateTenant because suspension is an operational action, not
// a configuration edit: it must not require the caller to resend the name and
// settings, which it might not have and could overwrite with stale values.
func (s *Store) SetTenantStatus(ctx context.Context, tenantID, status string) error {
	const q = `UPDATE tenants SET status = ? WHERE tenant_id = ?`

	res, err := s.db.ExecContext(ctx, q, status, tenantID)
	if err != nil {
		return fmt.Errorf("set status for tenant %s: %w", tenantID, err)
	}
	return requireAffected(res, fmt.Sprintf("tenant %s", tenantID))
}

// CreateAgentApp inserts an agent application.
//
// The tenant must exist. Checked here rather than left to a foreign key
// because the schema has none: the tables are deliberately independent so a
// tenant's data can be sharded or archived without cross-table constraints
// blocking it. That choice moves the check to this layer.
func (s *Store) CreateAgentApp(ctx context.Context, a *types.AgentApp) error {
	if a == nil {
		return errors.New("create agent app: nil app")
	}
	if err := s.requireTenant(ctx, a.TenantID); err != nil {
		return err
	}

	const q = `
INSERT INTO agent_apps (tenant_id, agent_app_id, name, description, status)
VALUES (?, ?, ?, ?, ?)`

	_, err := s.db.ExecContext(ctx, q,
		a.TenantID, a.AgentAppID, a.Name, nullString(a.Description), a.Status)
	if isDuplicate(err) {
		return fmt.Errorf("agent %s/%s: %w", a.TenantID, a.AgentAppID, ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("create agent app %s/%s: %w", a.TenantID, a.AgentAppID, err)
	}
	return nil
}

// UpdateAgentApp replaces an agent's mutable fields.
func (s *Store) UpdateAgentApp(ctx context.Context, a *types.AgentApp) error {
	if a == nil {
		return errors.New("update agent app: nil app")
	}

	const q = `
UPDATE agent_apps SET name = ?, description = ?, status = ?
 WHERE tenant_id = ? AND agent_app_id = ?`

	res, err := s.db.ExecContext(ctx, q,
		a.Name, nullString(a.Description), a.Status, a.TenantID, a.AgentAppID)
	if err != nil {
		return fmt.Errorf("update agent app %s/%s: %w", a.TenantID, a.AgentAppID, err)
	}
	return requireAffected(res, fmt.Sprintf("agent %s/%s", a.TenantID, a.AgentAppID))
}

// CreateAgentVersion inserts a draft version.
//
// Always a draft, regardless of what the caller asked for: a version has to be
// publishable only after its tools and extensions are attached, and inserting
// it as published would expose a half-configured agent to traffic for as long
// as the caller takes to finish wiring it.
func (s *Store) CreateAgentVersion(ctx context.Context, v *types.AgentVersion) error {
	if v == nil {
		return errors.New("create version: nil version")
	}
	if err := s.requireAgentApp(ctx, v.TenantID, v.AgentAppID); err != nil {
		return err
	}
	params, err := json.Marshal(orEmptyMap(v.ModelParams))
	if err != nil {
		return fmt.Errorf("encode model params for %s: %w", v.Version, err)
	}

	const q = `
INSERT INTO agent_versions
  (tenant_id, agent_app_id, version, status, system_prompt,
   model_name, model_api_key_ref, model_params, description)
VALUES (?, ?, ?, 'draft', ?, ?, ?, ?, ?)`

	_, err = s.db.ExecContext(ctx, q,
		v.TenantID, v.AgentAppID, v.Version, nullString(v.SystemPrompt),
		v.ModelName, nullString(v.ModelAPIKeyRef), string(params), nil)
	if isDuplicate(err) {
		return fmt.Errorf("version %s/%s/%s: %w",
			v.TenantID, v.AgentAppID, v.Version, ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("create version %s/%s/%s: %w",
			v.TenantID, v.AgentAppID, v.Version, err)
	}
	return nil
}

// UpdateDraftVersion edits a version that has not been published.
//
// The WHERE clause carries `status = 'draft'`, so publishing a version makes
// it immutable at the database level rather than by convention. A caller that
// tries anyway gets ErrNotFound, which is the honest answer: no *editable* row
// matched.
func (s *Store) UpdateDraftVersion(ctx context.Context, v *types.AgentVersion) error {
	if v == nil {
		return errors.New("update draft version: nil version")
	}
	params, err := json.Marshal(orEmptyMap(v.ModelParams))
	if err != nil {
		return fmt.Errorf("encode model params for %s: %w", v.Version, err)
	}

	const q = `
UPDATE agent_versions
   SET system_prompt = ?, model_name = ?, model_api_key_ref = ?, model_params = ?
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND status = 'draft'`

	res, err := s.db.ExecContext(ctx, q,
		nullString(v.SystemPrompt), v.ModelName, nullString(v.ModelAPIKeyRef),
		string(params), v.TenantID, v.AgentAppID, v.Version)
	if err != nil {
		return fmt.Errorf("update draft version %s/%s/%s: %w",
			v.TenantID, v.AgentAppID, v.Version, err)
	}
	return requireAffected(res,
		fmt.Sprintf("draft version %s/%s/%s", v.TenantID, v.AgentAppID, v.Version))
}

// PublishVersion moves a draft to published and stamps published_at.
//
// `status = 'draft'` in the WHERE makes this idempotent in the safe direction:
// publishing twice affects no rows the second time and returns ErrNotFound,
// rather than silently resetting published_at and losing the original date.
func (s *Store) PublishVersion(ctx context.Context, key types.RuntimeKey) error {
	const q = `
UPDATE agent_versions
   SET status = 'published', published_at = NOW(3)
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND status = 'draft'`

	res, err := s.db.ExecContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return fmt.Errorf("publish version %s: %w", key.String(), err)
	}
	return requireAffected(res, fmt.Sprintf("draft version %s", key.String()))
}

// ArchiveVersion retires a published version.
//
// Archiving does not touch deployments. A version still carrying traffic
// weight must be routed away first; the API layer enforces that, because the
// check needs the deployment row and belongs with the other validation rather
// than buried in a single UPDATE.
func (s *Store) ArchiveVersion(ctx context.Context, key types.RuntimeKey) error {
	const q = `
UPDATE agent_versions SET status = 'archived'
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? AND status = 'published'`

	res, err := s.db.ExecContext(ctx, q, key.TenantID, key.AgentAppID, key.AgentVersion)
	if err != nil {
		return fmt.Errorf("archive version %s: %w", key.String(), err)
	}
	return requireAffected(res, fmt.Sprintf("published version %s", key.String()))
}

// ReplaceToolBindings sets a draft version's tool permissions.
//
// Replacement in one transaction, not an incremental patch. The binding list
// is the whole permission grant: a partial update would leave a window in
// which an agent has a tool set nobody asked for, and on a live version that
// window is a permission escalation.
//
// Refuses non-draft versions for the same reason UpdateDraftVersion does — a
// published version's capabilities are frozen, so a cached Runtime cannot
// diverge from the database.
func (s *Store) ReplaceToolBindings(
	ctx context.Context,
	key types.RuntimeKey,
	bindings []types.ToolBinding,
) error {
	return s.replaceBindings(ctx, key, "agent_tool_bindings", func(tx *sql.Tx) error {
		const ins = `
INSERT INTO agent_tool_bindings (tenant_id, agent_app_id, version, tool_name, mode, params)
VALUES (?, ?, ?, ?, ?, ?)`

		for _, b := range bindings {
			params, err := json.Marshal(orEmptyMap(b.Params))
			if err != nil {
				return fmt.Errorf("encode params for tool %s: %w", b.ToolName, err)
			}
			if _, err := tx.ExecContext(ctx, ins,
				key.TenantID, key.AgentAppID, key.AgentVersion,
				b.ToolName, b.Mode, string(params)); err != nil {
				return fmt.Errorf("insert tool binding %s: %w", b.ToolName, err)
			}
		}
		return nil
	})
}

// ReplaceExtensionBindings sets a draft version's governance extensions.
//
// Priority is stored as given: it decides mount order, and order is
// load-bearing — redaction must run before anything that persists content, or
// unredacted text reaches the audit log.
func (s *Store) ReplaceExtensionBindings(
	ctx context.Context,
	key types.RuntimeKey,
	bindings []types.ExtensionBinding,
) error {
	return s.replaceBindings(ctx, key, "agent_extension_bindings", func(tx *sql.Tx) error {
		const ins = `
INSERT INTO agent_extension_bindings
  (tenant_id, agent_app_id, version, kind, extension_name, enabled, priority, params)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`

		for _, b := range bindings {
			params, err := json.Marshal(orEmptyMap(b.Params))
			if err != nil {
				return fmt.Errorf("encode params for extension %s: %w", b.ExtensionName, err)
			}
			if _, err := tx.ExecContext(ctx, ins,
				key.TenantID, key.AgentAppID, key.AgentVersion,
				b.Kind, b.ExtensionName, b.Enabled, b.Priority, string(params)); err != nil {
				return fmt.Errorf("insert extension binding %s: %w", b.ExtensionName, err)
			}
		}
		return nil
	})
}

// replaceBindings runs delete-then-insert for one binding table in a single
// transaction, after checking the version is still a draft.
//
// The draft check happens inside the transaction: checking outside would let a
// concurrent publish slip in between, and the bindings would then change under
// a version already serving traffic.
func (s *Store) replaceBindings(
	ctx context.Context,
	key types.RuntimeKey,
	table string,
	insert func(*sql.Tx) error,
) error {
	if !key.Valid() {
		return fmt.Errorf("replace %s: incomplete runtime key", table)
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx for %s: %w", table, err)
	}
	defer tx.Rollback()

	// FOR UPDATE so a concurrent publish blocks until this transaction ends.
	const lockQ = `
SELECT status FROM agent_versions
 WHERE tenant_id = ? AND agent_app_id = ? AND version = ? FOR UPDATE`

	var status string
	err = tx.QueryRowContext(ctx, lockQ,
		key.TenantID, key.AgentAppID, key.AgentVersion).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("version %s: %w", key.String(), ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("lock version %s: %w", key.String(), err)
	}
	if status != types.VersionStatusDraft {
		return fmt.Errorf("version %s is %s, only a draft's bindings may change: %w",
			key.String(), status, ErrNotFound)
	}

	// #nosec G202 -- table is one of two package-internal literals, never input.
	del := "DELETE FROM " + table +
		" WHERE tenant_id = ? AND agent_app_id = ? AND version = ?"
	if _, err := tx.ExecContext(ctx, del,
		key.TenantID, key.AgentAppID, key.AgentVersion); err != nil {
		return fmt.Errorf("clear %s for %s: %w", table, key.String(), err)
	}

	if err := insert(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit %s for %s: %w", table, key.String(), err)
	}
	return nil
}

// UpsertChannelBinding creates or updates an IM channel binding.
//
// Upsert rather than separate create and update: a binding is identified by an
// id the operator chooses, and re-applying the same configuration — from a
// deployment script, say — must not fail.
//
// The credential is never a parameter here, only SecretRef. Plaintext tokens
// live in the secret manager; a column holding one would end up in every
// backup and every SELECT *.
func (s *Store) UpsertChannelBinding(ctx context.Context, b *types.ChannelBinding) error {
	if b == nil {
		return errors.New("upsert channel binding: nil binding")
	}
	if err := s.requireAgentApp(ctx, b.TenantID, b.AgentAppID); err != nil {
		return err
	}
	caps, err := json.Marshal(b.Capabilities)
	if err != nil {
		return fmt.Errorf("encode capabilities for %s: %w", b.ChannelBindingID, err)
	}

	const q = `
INSERT INTO channel_bindings
  (channel_binding_id, tenant_id, agent_app_id, env, channel,
   external_app_id, webhook_path, secret_ref, capabilities, status)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) AS new
ON DUPLICATE KEY UPDATE
  agent_app_id    = new.agent_app_id,
  env             = new.env,
  channel         = new.channel,
  external_app_id = new.external_app_id,
  webhook_path    = new.webhook_path,
  secret_ref      = new.secret_ref,
  capabilities    = new.capabilities,
  status          = new.status`

	// webhook_path is NULL rather than "" for stream-mode bindings: uk_webhook
	// is a unique key, and several empty strings would collide while several
	// NULLs do not.
	_, err = s.db.ExecContext(ctx, q,
		b.ChannelBindingID, b.TenantID, b.AgentAppID, b.Env, b.Channel,
		nullString(b.ExternalAppID), nullString(b.WebhookPath),
		nullString(b.SecretRef), string(caps), b.Status)
	if isDuplicate(err) {
		// Not the primary key — that path is handled by the upsert. This is
		// uk_webhook: another binding already owns this callback path.
		return fmt.Errorf("webhook path %q already bound: %w", b.WebhookPath, ErrDuplicate)
	}
	if err != nil {
		return fmt.Errorf("upsert channel binding %s: %w", b.ChannelBindingID, err)
	}
	return nil
}

// SetChannelBindingStatus enables or disables a binding.
//
// Disabling is how an operator stops one IM entry point without deleting its
// configuration; Gateway's lookup filters on status = 'active'.
func (s *Store) SetChannelBindingStatus(ctx context.Context, tenantID, bindingID, status string) error {
	const q = `
UPDATE channel_bindings SET status = ?
 WHERE tenant_id = ? AND channel_binding_id = ?`

	res, err := s.db.ExecContext(ctx, q, status, tenantID, bindingID)
	if err != nil {
		return fmt.Errorf("set status for binding %s: %w", bindingID, err)
	}
	return requireAffected(res, fmt.Sprintf("binding %s/%s", tenantID, bindingID))
}

// requireTenant returns ErrNotFound unless the tenant exists.
func (s *Store) requireTenant(ctx context.Context, tenantID string) error {
	const q = `SELECT 1 FROM tenants WHERE tenant_id = ?`

	var one int
	err := s.db.QueryRowContext(ctx, q, tenantID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("tenant %s: %w", tenantID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("check tenant %s: %w", tenantID, err)
	}
	return nil
}

// requireAgentApp returns ErrNotFound unless the agent exists under the tenant.
//
// Both ids in the WHERE: this is also the cross-tenant check. Naming another
// tenant's agent id finds nothing rather than reaching it.
func (s *Store) requireAgentApp(ctx context.Context, tenantID, agentAppID string) error {
	const q = `SELECT 1 FROM agent_apps WHERE tenant_id = ? AND agent_app_id = ?`

	var one int
	err := s.db.QueryRowContext(ctx, q, tenantID, agentAppID).Scan(&one)
	if errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("agent %s/%s: %w", tenantID, agentAppID, ErrNotFound)
	}
	if err != nil {
		return fmt.Errorf("check agent %s/%s: %w", tenantID, agentAppID, err)
	}
	return nil
}

// requireAffected turns a zero-row update into ErrNotFound.
//
// Without this an UPDATE against a missing row succeeds silently, and the API
// answers 200 for a change that never happened.
func requireAffected(res sql.Result, what string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("rows affected for %s: %w", what, err)
	}
	if n == 0 {
		return fmt.Errorf("%s: %w", what, ErrNotFound)
	}
	return nil
}

// orEmptyMap substitutes an empty map for nil, so a JSON column holds `{}`
// rather than `null`. The readers use decodeJSON, which tolerates both, but a
// column that is sometimes null and sometimes an object makes every hand-written
// query that touches it need a COALESCE.
func orEmptyMap(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}
