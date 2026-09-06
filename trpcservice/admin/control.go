// 设计依据：docs/技术设计方案.md §4.2 Admin API
//                docs/多租户与节点部署设计.md §2「租户资源模型」
//                docs/功能开发计划.md 第一批「管理面写入」

package admin

import (
	"errors"
	"net/http"
	"regexp"

	"github.com/gin-gonic/gin"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// This file holds the write half of the control plane: creating tenants,
// agents and versions, and configuring their capabilities.
//
// The lifecycle these endpoints expose is deliberately narrow:
//
//	create tenant → create agent → create version (draft)
//	  → attach tools and extensions → publish → route traffic
//
// A draft is editable, a published version is not. That is enforced in SQL
// (see store.UpdateDraftVersion) rather than here, so a second caller cannot
// bypass it — but the endpoints follow the same shape so the error a client
// gets is about the version's state rather than about a failed query.
//
// Validation runs in three layers, and each catches something the others
// cannot:
//
//  1. shape — ids match a charset, required fields present, enums known;
//  2. business rules — the tenant exists, weights sum to 100, a version being
//     archived carries no traffic;
//  3. database constraints — unique keys, which are the only check that holds
//     under concurrency.
//
// Dropping any layer would let something through: shape checks cannot know
// whether a tenant exists, and a unique key cannot explain that a status value
// is misspelled.

// idPattern bounds the identifiers an operator may choose.
//
// These ids end up in Redis keys, log fields, metric labels and URL paths. A
// tenant id containing a colon would split a Redis key; one containing a slash
// would change which route matches. Restricting the charset here is cheaper
// than escaping at every use site.
var idPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_-]{0,63}$`)

// registerControl mounts the write endpoints.
func (a *API) registerControl(g *gin.RouterGroup) {
	g.POST("/tenants", a.createTenant)
	g.PUT("/tenants/:tenant", a.updateTenant)
	g.POST("/tenants/:tenant/status", a.setTenantStatus)

	g.POST("/tenants/:tenant/agents", a.createAgent)
	g.PUT("/tenants/:tenant/agents/:agent", a.updateAgent)

	g.POST("/tenants/:tenant/agents/:agent/versions", a.createVersion)
	g.PUT("/tenants/:tenant/agents/:agent/versions/:version", a.updateDraftVersion)
	g.POST("/tenants/:tenant/agents/:agent/versions/:version/publish", a.publishVersion)
	g.POST("/tenants/:tenant/agents/:agent/versions/:version/archive", a.archiveVersion)

	// Capability configuration is scoped to a version, not to an agent: tools
	// and governance belong to the immutable snapshot that a session freezes.
	g.PUT("/tenants/:tenant/agents/:agent/versions/:version/tools", a.replaceTools)
	g.PUT("/tenants/:tenant/agents/:agent/versions/:version/extensions", a.replaceExtensions)

	g.PUT("/tenants/:tenant/bindings/:binding", a.upsertBinding)
	g.POST("/tenants/:tenant/bindings/:binding/status", a.setBindingStatus)

	// The seventh element of the tenant model. Exposed as its own resource
	// rather than folded into the tenant body because changing it is an
	// audited security action, not a routine edit.
	g.GET("/tenants/:tenant/audit-policy", a.getAuditPolicy)
	g.PUT("/tenants/:tenant/audit-policy", a.putAuditPolicy)

	// Dangerous-tool confirmation. This is the channel that turns the
	// approval guardrail from "always refuse" into an actual gate.
	g.GET("/tenants/:tenant/approvals", a.listApprovals)
	g.POST("/tenants/:tenant/approvals/:approval/decide", a.decideApproval)
}

// ---------------------------------------------------------------------------
// Dangerous-tool approvals
// ---------------------------------------------------------------------------

// listApprovals shows the unanswered confirmation requests.
//
// Expired rows are filtered out by the store, so an operator is never offered
// a decision that can no longer take effect.
func (a *API) listApprovals(c *gin.Context) {
	rows, err := a.store.PendingToolApprovals(
		c.Request.Context(), c.Param("tenant"), limitFrom(c, 50))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"approvals": rows,
		"note":      "approving does not run the tool; the user must ask again",
	})
}

type decideApprovalRequest struct {
	// Decision is "approve" or "reject". Spelled as verbs rather than a
	// boolean so a malformed body cannot default to approval.
	Decision  string `json:"decision"`
	DecidedBy string `json:"decided_by"`
	Reason    string `json:"reason"`
}

// decideApproval answers one pending request.
//
// Approving does not execute anything. The tool runs on the *next* call that
// matches the approval, which is what keeps the decision out of the request
// path: blocking a Worker while a human decides would hold the session lease
// until it expired, handing the conversation to another Worker mid-wait.
//
// The response says so explicitly, because "I approved it, why did nothing
// happen" is the obvious first confusion.
func (a *API) decideApproval(c *gin.Context) {
	var req decideApprovalRequest
	if !bindJSON(c, &req) {
		return
	}
	if !required(c, "decided_by", req.DecidedBy) {
		return
	}

	var state types.ApprovalState
	switch req.Decision {
	case "approve":
		state = types.ApprovalApproved
	case "reject":
		state = types.ApprovalRejected
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "decision must be approve or reject, got " + req.Decision,
		})
		return
	}

	ctx := c.Request.Context()
	tenant, approvalID := c.Param("tenant"), c.Param("approval")

	// Read first so the tenant scope is checked before the update: without
	// it, one tenant could decide another's approval by guessing an id.
	existing, err := a.store.ToolApprovalByID(ctx, tenant, approvalID)
	if a.fail(c, err) {
		return
	}
	if existing.State != types.ApprovalPending {
		// Already answered. Refusing a second decision is what stops a
		// rejected call from being quietly re-approved.
		c.JSON(http.StatusConflict, gin.H{
			"error": "approval is already " + string(existing.State),
		})
		return
	}

	if err := a.store.ResolveToolApproval(ctx, approvalID, state, req.DecidedBy, req.Reason); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			// Lost a race with another decider between the read and the
			// update. The other decision stands.
			c.JSON(http.StatusConflict, gin.H{"error": "approval was decided concurrently"})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Warn("tool approval decided",
		"tenant_id", tenant, "approval_id", approvalID,
		"tool", existing.ToolName, "decision", state, "decided_by", req.DecidedBy)

	note := "rejected; the tool will stay refused"
	if state == types.ApprovalApproved {
		note = "approved for one execution of these exact arguments; " +
			"ask the agent again to run it"
	}
	c.JSON(http.StatusOK, gin.H{
		"approval_id": approvalID,
		"tool_name":   existing.ToolName,
		"state":       state,
		"note":        note,
	})
}

// ---------------------------------------------------------------------------
// Audit policy — tenant model element 7
// ---------------------------------------------------------------------------

func (a *API) getAuditPolicy(c *gin.Context) {
	p, err := a.store.AuditPolicy(c.Request.Context(), c.Param("tenant"))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, p)
}

type auditPolicyRequest struct {
	RedactLevel   string `json:"redact_level"`
	BodyMode      string `json:"body_mode"`
	BodyMaxChars  int    `json:"body_max_chars"`
	RetentionDays int    `json:"retention_days"`
}

// putAuditPolicy sets how much user-authored text survives into the audit log.
//
// Note what this cannot do: it never affects credential scrubbing. Secrets are
// stripped unconditionally, so no policy — including redact_level=none — can
// cause an API key to be logged. A tenant configuring its own audit must not
// be able to opt into leaking the platform's secrets.
func (a *API) putAuditPolicy(c *gin.Context) {
	var req auditPolicyRequest
	if !bindJSON(c, &req) {
		return
	}

	p := types.DefaultAuditPolicy(c.Param("tenant"))
	if req.RedactLevel != "" {
		p.RedactLevel = types.RedactLevel(req.RedactLevel)
	}
	if req.BodyMode != "" {
		p.BodyMode = types.BodyMode(req.BodyMode)
	}
	if req.BodyMaxChars > 0 {
		p.BodyMaxChars = req.BodyMaxChars
	}
	if req.RetentionDays > 0 {
		p.RetentionDays = req.RetentionDays
	}

	// Validation errors are the client's mistake, so they must not surface as
	// a 500. Valid() names the offending field.
	if err := p.Valid(); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if err := a.store.UpsertAuditPolicy(c.Request.Context(), p); a.failWrite(c, err) {
		return
	}

	a.log.Info("audit policy updated",
		"tenant_id", p.TenantID, "redact_level", p.RedactLevel,
		"body_mode", p.BodyMode, "retention_days", p.RetentionDays)
	c.JSON(http.StatusOK, gin.H{
		"audit_policy": p,
		"note":         "credential scrubbing is unconditional and unaffected by this setting",
	})
}

// ---------------------------------------------------------------------------
// Tenants
// ---------------------------------------------------------------------------

type createTenantRequest struct {
	TenantID string         `json:"tenant_id"`
	Name     string         `json:"name"`
	Settings map[string]any `json:"settings"`
}

func (a *API) createTenant(c *gin.Context) {
	var req createTenantRequest
	if !bindJSON(c, &req) {
		return
	}
	if !validID(c, "tenant_id", req.TenantID) || !required(c, "name", req.Name) {
		return
	}

	t := &types.Tenant{
		TenantID: req.TenantID,
		Name:     req.Name,
		Status:   types.StatusActive,
		Settings: req.Settings,
	}
	ctx := c.Request.Context()
	if err := a.store.CreateTenant(ctx, t); a.failWrite(c, err) {
		return
	}

	// The seventh element of the tenant model, written with the tenant rather
	// than left absent. A tenant with no policy falls back to a safe default,
	// so this is not load-bearing for correctness — but an explicit row is
	// what makes the setting discoverable, and a setting nobody can see is a
	// setting nobody configures.
	policy := types.DefaultAuditPolicy(t.TenantID)
	if err := a.store.UpsertAuditPolicy(ctx, policy); err != nil {
		// The tenant exists and is usable; only its policy row is missing,
		// and reads fall back to the same default. Logging beats failing the
		// creation and leaving the caller unsure whether it succeeded.
		a.log.Warn("tenant created but audit policy not written",
			"tenant_id", t.TenantID, "error", applog.Scrub(err.Error()))
	}

	a.log.Info("tenant created", "tenant_id", t.TenantID)
	c.JSON(http.StatusCreated, gin.H{"tenant": t, "audit_policy": policy})
}

type updateTenantRequest struct {
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	Settings map[string]any `json:"settings"`
}

func (a *API) updateTenant(c *gin.Context) {
	var req updateTenantRequest
	if !bindJSON(c, &req) {
		return
	}
	if !required(c, "name", req.Name) {
		return
	}
	if req.Status == "" {
		req.Status = types.StatusActive
	}
	if !validStatus(c, req.Status) {
		return
	}

	t := &types.Tenant{
		TenantID: c.Param("tenant"),
		Name:     req.Name,
		Status:   req.Status,
		Settings: req.Settings,
	}
	if err := a.store.UpdateTenant(c.Request.Context(), t); a.failWrite(c, err) {
		return
	}

	a.log.Info("tenant updated", "tenant_id", t.TenantID, "status", t.Status)
	c.JSON(http.StatusOK, t)
}

type statusRequest struct {
	Status string `json:"status"`
}

// setTenantStatus suspends or reactivates a tenant.
//
// Suspension does not stop in-flight work. A message already in a mailbox will
// still be processed: the alternative is discarding it, and a suspended tenant
// is usually a billing state rather than a security incident. Gateway rejects
// *new* inbound messages for a suspended tenant, which is where the boundary
// belongs.
func (a *API) setTenantStatus(c *gin.Context) {
	var req statusRequest
	if !bindJSON(c, &req) || !validStatus(c, req.Status) {
		return
	}

	tenant := c.Param("tenant")
	if err := a.store.SetTenantStatus(c.Request.Context(), tenant, req.Status); a.failWrite(c, err) {
		return
	}

	a.log.Info("tenant status changed", "tenant_id", tenant, "status", req.Status)
	c.JSON(http.StatusOK, gin.H{
		"tenant_id": tenant,
		"status":    req.Status,
		"note":      "new inbound messages are refused; work already queued still completes",
	})
}

// ---------------------------------------------------------------------------
// Agent applications
// ---------------------------------------------------------------------------

type createAgentRequest struct {
	AgentAppID  string `json:"agent_app_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

func (a *API) createAgent(c *gin.Context) {
	var req createAgentRequest
	if !bindJSON(c, &req) {
		return
	}
	if !validID(c, "agent_app_id", req.AgentAppID) || !required(c, "name", req.Name) {
		return
	}

	app := &types.AgentApp{
		TenantID:    c.Param("tenant"),
		AgentAppID:  req.AgentAppID,
		Name:        req.Name,
		Description: req.Description,
		Status:      types.StatusActive,
	}
	if err := a.store.CreateAgentApp(c.Request.Context(), app); a.failWrite(c, err) {
		return
	}

	a.log.Info("agent app created",
		"tenant_id", app.TenantID, "agent_app_id", app.AgentAppID)
	c.JSON(http.StatusCreated, app)
}

type updateAgentRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Status      string `json:"status"`
}

func (a *API) updateAgent(c *gin.Context) {
	var req updateAgentRequest
	if !bindJSON(c, &req) {
		return
	}
	if !required(c, "name", req.Name) {
		return
	}
	if req.Status == "" {
		req.Status = types.StatusActive
	}
	if !validStatus(c, req.Status) {
		return
	}

	app := &types.AgentApp{
		TenantID:    c.Param("tenant"),
		AgentAppID:  c.Param("agent"),
		Name:        req.Name,
		Description: req.Description,
		Status:      req.Status,
	}
	if err := a.store.UpdateAgentApp(c.Request.Context(), app); a.failWrite(c, err) {
		return
	}

	a.log.Info("agent app updated",
		"tenant_id", app.TenantID, "agent_app_id", app.AgentAppID, "status", app.Status)
	c.JSON(http.StatusOK, app)
}

// ---------------------------------------------------------------------------
// Versions
// ---------------------------------------------------------------------------

type createVersionRequest struct {
	Version        string         `json:"version"`
	SystemPrompt   string         `json:"system_prompt"`
	ModelName      string         `json:"model_name"`
	ModelAPIKeyRef string         `json:"model_api_key_ref"`
	ModelParams    map[string]any `json:"model_params"`
}

// createVersion inserts a draft.
//
// Always a draft: a version becomes publishable only once its tools and
// extensions are attached, and creating it published would expose a
// half-configured agent for as long as the caller takes to finish.
func (a *API) createVersion(c *gin.Context) {
	var req createVersionRequest
	if !bindJSON(c, &req) {
		return
	}
	if !validID(c, "version", req.Version) || !required(c, "model_name", req.ModelName) {
		return
	}

	v := &types.AgentVersion{
		TenantID:       c.Param("tenant"),
		AgentAppID:     c.Param("agent"),
		Version:        req.Version,
		Status:         types.VersionStatusDraft,
		SystemPrompt:   req.SystemPrompt,
		ModelName:      req.ModelName,
		ModelAPIKeyRef: req.ModelAPIKeyRef,
		ModelParams:    req.ModelParams,
	}
	if err := a.store.CreateAgentVersion(c.Request.Context(), v); a.failWrite(c, err) {
		return
	}

	a.log.Info("version created",
		"tenant_id", v.TenantID, "agent_app_id", v.AgentAppID, "version", v.Version)
	c.JSON(http.StatusCreated, gin.H{
		"version": v,
		"note":    "draft: attach tools and extensions, then publish",
	})
}

func (a *API) updateDraftVersion(c *gin.Context) {
	var req createVersionRequest
	if !bindJSON(c, &req) {
		return
	}
	if !required(c, "model_name", req.ModelName) {
		return
	}

	v := &types.AgentVersion{
		TenantID:       c.Param("tenant"),
		AgentAppID:     c.Param("agent"),
		Version:        c.Param("version"),
		SystemPrompt:   req.SystemPrompt,
		ModelName:      req.ModelName,
		ModelAPIKeyRef: req.ModelAPIKeyRef,
		ModelParams:    req.ModelParams,
	}
	// ErrNotFound here can mean either "no such version" or "it is published".
	// The message says both, because the caller cannot tell them apart from a
	// bare 404 and the second is by far the likelier mistake.
	if err := a.store.UpdateDraftVersion(c.Request.Context(), v); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "no editable draft: the version is missing or already published",
				"note":  "published versions are immutable; create a new version instead",
			})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Info("draft version updated",
		"tenant_id", v.TenantID, "agent_app_id", v.AgentAppID, "version", v.Version)
	c.JSON(http.StatusOK, v)
}

// publishVersion makes a draft eligible for traffic.
//
// Publishing does not route anything: weights are a separate call. Keeping
// them apart is what allows a version to be published, verified, and only then
// given traffic — and it is why a rollback never needs to un-publish.
func (a *API) publishVersion(c *gin.Context) {
	key := a.versionKey(c)

	if err := a.store.PublishVersion(c.Request.Context(), key); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "no draft to publish: the version is missing or already published",
			})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Info("version published", "key", key.String())
	c.JSON(http.StatusOK, gin.H{
		"version": key.AgentVersion,
		"status":  types.VersionStatusPublished,
		"note":    "published but not routed; set deployment weights to send traffic",
	})
}

// archiveVersion retires a published version.
//
// Refuses while the version still carries traffic weight. Archiving it anyway
// would leave the deployment pointing at a version the assembler rejects, and
// the failure would surface as user-visible errors instead of as a rejected
// request — the same reasoning as the weight validation in updateDeployment.
func (a *API) archiveVersion(c *gin.Context) {
	ctx := c.Request.Context()
	key := a.versionKey(c)
	env := c.DefaultQuery("env", "prod")

	d, err := a.store.Deployment(ctx, key.TenantID, key.AgentAppID, env)
	switch {
	case err == nil:
		for _, r := range d.Routes {
			if r.Version == key.AgentVersion && r.Weight > 0 {
				c.JSON(http.StatusConflict, gin.H{
					"error": "version still carries traffic weight",
					"note":  "route traffic away first, then archive",
				})
				return
			}
		}
	case errors.Is(err, store.ErrNotFound):
		// No deployment for this environment, so nothing can be routed to it.
	default:
		a.failWrite(c, err)
		return
	}

	if err := a.store.ArchiveVersion(ctx, key); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "no published version to archive",
			})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Info("version archived", "key", key.String())
	c.JSON(http.StatusOK, gin.H{"version": key.AgentVersion, "status": types.VersionStatusArchived})
}

// ---------------------------------------------------------------------------
// Capability bindings
// ---------------------------------------------------------------------------

type replaceToolsRequest struct {
	Tools []types.ToolBinding `json:"tools"`
}

// replaceTools sets a draft's tool permissions as a whole set.
//
// Replacement, not a patch: the list *is* the permission grant. A partial
// update would leave a window in which the agent holds a tool set nobody
// asked for.
func (a *API) replaceTools(c *gin.Context) {
	var req replaceToolsRequest
	if !bindJSON(c, &req) {
		return
	}
	for _, t := range req.Tools {
		if !required(c, "tool_name", t.ToolName) {
			return
		}
		switch t.Mode {
		case types.ToolModeAllow, types.ToolModeDeny, types.ToolModeAsk:
		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "tool " + t.ToolName + " has unknown mode " + string(t.Mode),
				"note":  "mode must be allow, deny or ask",
			})
			return
		}
	}

	key := a.versionKey(c)
	if err := a.store.ReplaceToolBindings(c.Request.Context(), key, req.Tools); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "version is missing or not a draft; bindings of a published version are frozen",
			})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Info("tool bindings replaced", "key", key.String(), "count", len(req.Tools))
	c.JSON(http.StatusOK, gin.H{"tools": req.Tools})
}

type replaceExtensionsRequest struct {
	Extensions []types.ExtensionBinding `json:"extensions"`
}

// replaceExtensions sets a draft's governance extensions.
//
// Priority is accepted as given because mount order is load-bearing:
// redaction must run before anything that persists content, or unredacted
// text reaches the audit log. Reordering on the platform's behalf would hide
// that constraint rather than enforce it.
func (a *API) replaceExtensions(c *gin.Context) {
	var req replaceExtensionsRequest
	if !bindJSON(c, &req) {
		return
	}
	for _, e := range req.Extensions {
		if !required(c, "extension_name", e.ExtensionName) {
			return
		}
		switch e.Kind {
		case types.ExtensionKindPlugin, types.ExtensionKindGuardrail, types.ExtensionKindCallback:
		default:
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "extension " + e.ExtensionName + " has unknown kind " + string(e.Kind),
				"note":  "kind must be plugin, guardrail or callback",
			})
			return
		}
	}

	key := a.versionKey(c)
	if err := a.store.ReplaceExtensionBindings(c.Request.Context(), key, req.Extensions); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusConflict, gin.H{
				"error": "version is missing or not a draft; bindings of a published version are frozen",
			})
			return
		}
		a.failWrite(c, err)
		return
	}

	a.log.Info("extension bindings replaced", "key", key.String(), "count", len(req.Extensions))
	c.JSON(http.StatusOK, gin.H{"extensions": req.Extensions})
}

// ---------------------------------------------------------------------------
// Channel bindings
// ---------------------------------------------------------------------------

type upsertBindingRequest struct {
	AgentAppID    string             `json:"agent_app_id"`
	Env           string             `json:"env"`
	Channel       string             `json:"channel"`
	ExternalAppID string             `json:"external_app_id"`
	WebhookPath   string             `json:"webhook_path"`
	SecretRef     string             `json:"secret_ref"`
	Capabilities  types.Capabilities `json:"capabilities"`
	Status        string             `json:"status"`
}

// upsertBinding creates or replaces an IM channel binding.
//
// Only SecretRef is accepted, never a credential. A plaintext token in a
// request body would end up in access logs; behind a reference it stays in the
// secret manager.
func (a *API) upsertBinding(c *gin.Context) {
	var req upsertBindingRequest
	if !bindJSON(c, &req) {
		return
	}
	if !required(c, "agent_app_id", req.AgentAppID) || !required(c, "channel", req.Channel) {
		return
	}
	if req.Env == "" {
		req.Env = "prod"
	}
	if req.Status == "" {
		req.Status = types.StatusActive
	}
	if !validStatus(c, req.Status) {
		return
	}

	// The two inbound modes differ in what they require, and getting this
	// wrong produces a binding that silently never receives anything: a
	// callback binding with no path is unreachable, and a stream binding with
	// one would suggest a webhook that is never served.
	switch req.Capabilities.InboundMode {
	case types.InboundModeStream:
		if req.WebhookPath != "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "stream bindings must not set webhook_path",
				"note":  "the platform dials out; there is no callback URL",
			})
			return
		}
	case types.InboundModePayload, types.InboundModeFetch:
		if req.WebhookPath == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "callback bindings require webhook_path",
			})
			return
		}
	case "":
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "capabilities.inbound_mode is required",
			"note":  "one of payload, fetch, stream",
		})
		return
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "unknown inbound_mode " + string(req.Capabilities.InboundMode),
		})
		return
	}

	b := &types.ChannelBinding{
		ChannelBindingID: c.Param("binding"),
		TenantID:         c.Param("tenant"),
		AgentAppID:       req.AgentAppID,
		Env:              req.Env,
		Channel:          req.Channel,
		ExternalAppID:    req.ExternalAppID,
		WebhookPath:      req.WebhookPath,
		SecretRef:        req.SecretRef,
		Capabilities:     req.Capabilities,
		Status:           req.Status,
	}
	if !validID(c, "binding", b.ChannelBindingID) {
		return
	}
	if err := a.store.UpsertChannelBinding(c.Request.Context(), b); a.failWrite(c, err) {
		return
	}

	a.log.Info("channel binding upserted",
		"tenant_id", b.TenantID, "channel_binding_id", b.ChannelBindingID,
		"channel", b.Channel, "inbound_mode", b.Capabilities.InboundMode)

	note := "callback bindings take effect immediately"
	if b.Capabilities.StreamCapable() {
		// Honest about a real limitation rather than letting an operator
		// wonder why nothing connects.
		note = "stream bindings are read at startup; restart Gateway to pick this up"
	}
	c.JSON(http.StatusOK, gin.H{"binding": b, "note": note})
}

func (a *API) setBindingStatus(c *gin.Context) {
	var req statusRequest
	if !bindJSON(c, &req) || !validStatus(c, req.Status) {
		return
	}

	tenant, binding := c.Param("tenant"), c.Param("binding")
	if err := a.store.SetChannelBindingStatus(
		c.Request.Context(), tenant, binding, req.Status); a.failWrite(c, err) {
		return
	}

	a.log.Info("channel binding status changed",
		"tenant_id", tenant, "channel_binding_id", binding, "status", req.Status)
	c.JSON(http.StatusOK, gin.H{"channel_binding_id": binding, "status": req.Status})
}

// ---------------------------------------------------------------------------
// Shared validation
// ---------------------------------------------------------------------------

func (a *API) versionKey(c *gin.Context) types.RuntimeKey {
	return types.RuntimeKey{
		TenantID:     c.Param("tenant"),
		AgentAppID:   c.Param("agent"),
		AgentVersion: c.Param("version"),
	}
}

func bindJSON(c *gin.Context, out any) bool {
	if err := c.ShouldBindJSON(out); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed body: " + err.Error()})
		return false
	}
	return true
}

func required(c *gin.Context, field, value string) bool {
	if value == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": field + " is required"})
		return false
	}
	return true
}

// validID rejects identifiers that would break the systems they flow into.
//
// These values become Redis key segments, log fields, metric labels and URL
// path components. A colon splits a Redis key; a slash changes route matching.
// Rejecting them once here is cheaper and safer than escaping everywhere.
func validID(c *gin.Context, field, value string) bool {
	if !required(c, field, value) {
		return false
	}
	if !idPattern.MatchString(value) {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": field + " must match " + idPattern.String(),
			"note":  "ids become Redis keys, metric labels and URL segments",
		})
		return false
	}
	return true
}

func validStatus(c *gin.Context, status string) bool {
	switch status {
	case types.StatusActive, types.StatusSuspended:
		return true
	default:
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "status must be active or suspended, got " + status,
		})
		return false
	}
}

// failWrite maps store errors onto status codes.
//
// Duplicate is 409 rather than 500: "this id is taken" is a client mistake
// with an obvious fix, and reporting it as an internal error would send an
// operator looking at the database instead of at their request.
func (a *API) failWrite(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrDuplicate) {
		c.JSON(http.StatusConflict, gin.H{"error": "already exists"})
		return true
	}
	return a.fail(c, err)
}
