// 设计依据：docs/技术设计方案.md §4.2「Gateway、Scheduler 和 Worker」Admin API
//                docs/故障恢复与运维设计.md 灰度发布和回滚
//                docs/dev/技术栈说明.md §3「Web 框架：Gin」

// Package admin serves the control-plane API.
//
// It runs inside Gateway because Gateway is already the process with a public
// listener and a database connection; Workers expose nothing callable.
//
// Two properties shape the handlers:
//
//   - Published versions are immutable. There is no endpoint that edits one.
//     Changing a prompt, a model or a tool means creating a new version, which
//     is what keeps a cached Runtime from ever being stale and what makes a
//     rollback a weight change rather than a data migration.
//   - Rollback is a single-row update. Weights live in one JSON column on one
//     deployment row, so adjusting them is atomic and there is no window in
//     which they sum to something other than 100.
package admin

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// DeadLetterStore exposes parked messages for inspection and replay.
type DeadLetterStore interface {
	ListDeadLetters(ctx context.Context, sessionID string, limit int64) ([]scheduler.DeadLetter, error)
	DeadLetterCount(ctx context.Context, sessionID string) (int64, error)
	ReplayDeadLetter(ctx context.Context, sessionID string) (*types.InboundMessage, error)
}

// API serves the admin endpoints.
type API struct {
	store      *store.Store
	deadLetter DeadLetterStore
	log        *slog.Logger
}

// New builds the API.
func New(s *store.Store, dl DeadLetterStore, logger *slog.Logger) *API {
	if logger == nil {
		logger = slog.Default()
	}
	return &API{store: s, deadLetter: dl, log: logger}
}

// Register mounts the routes under /admin.
//
// Authentication is deliberately absent at this layer: an admin API must sit
// behind the platform's own identity provider, and a token check invented here
// would be security theatre that discourages wiring the real one.
func (a *API) Register(r *gin.Engine) {
	g := r.Group("/admin")

	g.GET("/tenants", a.listTenants)
	g.GET("/tenants/:tenant", a.getTenant)

	g.GET("/tenants/:tenant/agents", a.listAgents)
	g.GET("/tenants/:tenant/agents/:agent/versions", a.listVersions)
	g.GET("/tenants/:tenant/agents/:agent/versions/:version", a.getVersion)

	g.GET("/tenants/:tenant/agents/:agent/deployment", a.getDeployment)
	g.PUT("/tenants/:tenant/agents/:agent/deployment", a.updateDeployment)

	g.GET("/tenants/:tenant/bindings", a.listBindings)
	g.GET("/tenants/:tenant/sessions", a.listSessions)
	g.GET("/tenants/:tenant/audit", a.listAudit)
	g.GET("/tenants/:tenant/usage", a.usageSummary)

	// Dead letters are keyed by session rather than by tenant: a parked
	// message belongs to one conversation, and replaying it has to put it
	// back into that conversation's mailbox.
	g.GET("/sessions/:session/deadletters", a.listDeadLetters)
	g.POST("/sessions/:session/deadletters/replay", a.replayDeadLetter)
}

func (a *API) listDeadLetters(c *gin.Context) {
	if a.deadLetter == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "dead letter store not configured"})
		return
	}
	session := c.Param("session")

	rows, err := a.deadLetter.ListDeadLetters(c.Request.Context(), session, int64(limitFrom(c, 20)))
	if a.fail(c, err) {
		return
	}
	total, err := a.deadLetter.DeadLetterCount(c.Request.Context(), session)
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"session_id": session, "total": total, "dead_letters": rows})
}

// replayDeadLetter returns the oldest parked message to the mailbox.
//
// One per call rather than a bulk drain: a replay is only correct once the
// cause has been fixed, and replaying a batch of messages that will all fail
// again just refills the dead letter while burning model quota.
func (a *API) replayDeadLetter(c *gin.Context) {
	if a.deadLetter == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "dead letter store not configured"})
		return
	}
	session := c.Param("session")

	msg, err := a.deadLetter.ReplayDeadLetter(c.Request.Context(), session)
	if a.fail(c, err) {
		return
	}
	if msg == nil {
		c.JSON(http.StatusOK, gin.H{"replayed": false, "reason": "no dead letters"})
		return
	}

	a.log.Info("dead letter replayed",
		"session_id", session, "request_id", msg.RequestID)

	c.JSON(http.StatusOK, gin.H{
		"replayed":   true,
		"request_id": msg.RequestID,
		"note":       "queued behind any newer messages; a Worker picks it up on the next hint",
	})
}

func (a *API) listTenants(c *gin.Context) {
	rows, err := a.store.ListTenants(c.Request.Context())
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"tenants": rows})
}

func (a *API) getTenant(c *gin.Context) {
	t, err := a.store.TenantByID(c.Request.Context(), c.Param("tenant"))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, t)
}

func (a *API) listAgents(c *gin.Context) {
	rows, err := a.store.ListAgentApps(c.Request.Context(), c.Param("tenant"))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"agents": rows})
}

func (a *API) listVersions(c *gin.Context) {
	rows, err := a.store.ListAgentVersions(c.Request.Context(), c.Param("tenant"), c.Param("agent"))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"versions": rows})
}

// getVersion returns a version with its capability bindings, which is the
// exact input the assembler reads. Exposing it makes "why does this agent
// behave this way" answerable without reading five tables by hand.
func (a *API) getVersion(c *gin.Context) {
	key := types.RuntimeKey{
		TenantID:     c.Param("tenant"),
		AgentAppID:   c.Param("agent"),
		AgentVersion: c.Param("version"),
	}
	spec, err := a.store.RuntimeSpec(c.Request.Context(), key)
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, spec)
}

func (a *API) getDeployment(c *gin.Context) {
	env := c.DefaultQuery("env", "prod")
	d, err := a.store.Deployment(c.Request.Context(), c.Param("tenant"), c.Param("agent"), env)
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, d)
}

// updateDeploymentRequest is a full replacement of the routing weights.
//
// Replacement rather than a patch: the weights are meaningful only as a set,
// and a partial update would allow a state where they no longer sum to 100.
type updateDeploymentRequest struct {
	Env       string               `json:"env"`
	Routes    []types.VersionRoute `json:"routes"`
	UpdatedBy string               `json:"updated_by"`
}

// updateDeployment adjusts traffic weights. This one endpoint covers both
// gradual rollout and rollback: shifting weight to a new version is a rollout,
// shifting it back is a rollback, and both are the same single-row update.
//
// Sessions already under way are unaffected — each froze its version at
// creation — so a rollback changes where new conversations go, not what an
// in-flight one is doing.
func (a *API) updateDeployment(c *gin.Context) {
	var req updateDeploymentRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "malformed body: " + err.Error()})
		return
	}
	if req.Env == "" {
		req.Env = "prod"
	}
	if len(req.Routes) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "routes must not be empty"})
		return
	}

	ctx := c.Request.Context()
	tenant, agent := c.Param("tenant"), c.Param("agent")

	// Every named version must exist and be published. Without this check a
	// typo would route live traffic to a version that cannot be assembled,
	// and the failure would surface as user-visible errors rather than as a
	// rejected request.
	total := 0
	for _, r := range req.Routes {
		if r.Weight < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "weight must not be negative"})
			return
		}
		total += r.Weight

		v, err := a.store.AgentVersion(ctx, types.RuntimeKey{
			TenantID: tenant, AgentAppID: agent, AgentVersion: r.Version,
		})
		if errors.Is(err, store.ErrNotFound) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "unknown version " + r.Version})
			return
		}
		if a.fail(c, err) {
			return
		}
		if !v.Published() {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "version " + r.Version + " is " + v.Status + ", not published",
			})
			return
		}
	}
	if total != 100 {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "weights must sum to 100, got " + strconv.Itoa(total),
		})
		return
	}

	d := &types.Deployment{
		TenantID: tenant, AgentAppID: agent, Env: req.Env, Routes: req.Routes,
	}
	if err := a.store.UpdateDeployment(ctx, d, req.UpdatedBy); a.fail(c, err) {
		return
	}

	a.log.Info("deployment weights updated",
		"tenant_id", tenant, "agent_app_id", agent, "env", req.Env,
		"routes", req.Routes, "updated_by", req.UpdatedBy)

	c.JSON(http.StatusOK, gin.H{
		"deployment": d,
		"note":       "in-flight sessions keep the version they were created with",
	})
}

func (a *API) listBindings(c *gin.Context) {
	rows, err := a.store.ListChannelBindings(c.Request.Context(), c.Param("tenant"))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"bindings": rows})
}

func (a *API) listSessions(c *gin.Context) {
	rows, err := a.store.ListSessions(c.Request.Context(), c.Param("tenant"), limitFrom(c, 50))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"sessions": rows})
}

func (a *API) listAudit(c *gin.Context) {
	rows, err := a.store.ListAudit(c.Request.Context(), c.Param("tenant"), limitFrom(c, 100))
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, gin.H{"records": rows})
}

// usageSummary aggregates spend over a window, for a per-tenant cost view.
func (a *API) usageSummary(c *gin.Context) {
	since := time.Now().Add(-24 * time.Hour)
	if d := c.Query("since"); d != "" {
		if parsed, err := time.ParseDuration(d); err == nil {
			since = time.Now().Add(-parsed)
		}
	}
	sum, err := a.store.UsageSummary(c.Request.Context(), c.Param("tenant"), since)
	if a.fail(c, err) {
		return
	}
	c.JSON(http.StatusOK, sum)
}

// fail writes an error response and reports whether it did.
//
// Messages are scrubbed: an admin API returns database errors verbatim
// otherwise, and a failed connection string is exactly the kind of text that
// carries a password.
func (a *API) fail(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrNotFound) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return true
	}
	a.log.Error("admin request failed",
		"path", c.Request.URL.Path, "error", applog.Scrub(err.Error()))
	c.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
	return true
}

func limitFrom(c *gin.Context, def int) int {
	n, err := strconv.Atoi(c.Query("limit"))
	if err != nil || n <= 0 {
		return def
	}
	// Bounded so one request cannot pull an entire event history into memory.
	if n > 500 {
		return 500
	}
	return n
}
