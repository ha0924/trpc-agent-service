// 设计依据：docs/技术设计方案.md §4.2「Gateway、Scheduler 和 Worker」
//                docs/dev/技术栈说明.md §3「Web 框架：Gin」

// Package web serves the operator console: a chat page and a management page.
//
// The chat page deliberately posts to the **real** webhook endpoint rather
// than to a private shortcut. It therefore exercises the production path —
// signature verification, idempotency, ACK, queue, lease, assembly, delivery —
// and a console that works proves the platform works. A private path would
// only prove that the private path works.
//
// The consequence is that the reply is asynchronous: the ACK carries no answer.
// The page polls session_events for it, and polls the inbound state alongside,
// because otherwise a browser cannot tell a slow model call apart from a
// request that already failed and would wait forever on the latter.
package web

import (
	"errors"
	"html/template"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"

	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
)

// Console serves the pages and the small API they need.
type Console struct {
	store *store.Store
	log   *slog.Logger
}

// New builds the console.
func New(s *store.Store, logger *slog.Logger) *Console {
	if logger == nil {
		logger = slog.Default()
	}
	return &Console{store: s, log: logger}
}

// Register mounts the console.
//
// It reuses the admin API for configuration reads instead of duplicating those
// queries, and adds only what a browser cannot get from there: conversation
// history and per-request state.
func (c *Console) Register(r *gin.Engine) {
	r.SetHTMLTemplate(template.Must(template.New("console").Parse(consoleHTML)))

	r.GET("/", func(ctx *gin.Context) { ctx.Redirect(http.StatusFound, "/console") })
	r.GET("/console", c.page)

	api := r.Group("/console/api")
	api.GET("/sessions/:session/events", c.sessionEvents)
	api.GET("/requests/:request/state", c.requestState)
	api.GET("/overview", c.overview)
}

func (c *Console) page(ctx *gin.Context) {
	ctx.HTML(http.StatusOK, "console", nil)
}

// sessionEvents returns a conversation for the chat pane.
func (c *Console) sessionEvents(ctx *gin.Context) {
	tenant := ctx.Query("tenant")
	if tenant == "" {
		ctx.JSON(http.StatusBadRequest, gin.H{"error": "tenant is required"})
		return
	}

	limit := 200
	if n, err := strconv.Atoi(ctx.Query("limit")); err == nil && n > 0 && n <= 500 {
		limit = n
	}

	rows, err := c.store.SessionEvents(ctx.Request.Context(), tenant, ctx.Param("session"), limit)
	if c.fail(ctx, err) {
		return
	}
	ctx.JSON(http.StatusOK, gin.H{"events": rows})
}

// requestState lets the page stop polling.
//
// A browser waiting for a reply needs to know whether the work is still under
// way. Without this it cannot distinguish "the model is slow" from "this died
// three minutes ago", and would spin until the user gives up.
func (c *Console) requestState(ctx *gin.Context) {
	state, lastErr, err := c.store.InboundState(ctx.Request.Context(), ctx.Param("request"))
	if c.fail(ctx, err) {
		return
	}
	ctx.JSON(http.StatusOK, gin.H{
		"state": state,
		"error": lastErr,
		// Terminal states tell the page to stop polling. delivery_failed is
		// deliberately not terminal: the sweeper retries the delivery, so the
		// reply may still arrive.
		"terminal": state == types.StateSucceeded || state == types.StateFailed,
	})
}

// overview is the management page's summary.
func (c *Console) overview(ctx *gin.Context) {
	reqCtx := ctx.Request.Context()

	tenants, err := c.store.ListTenants(reqCtx)
	if c.fail(ctx, err) {
		return
	}

	type tenantView struct {
		types.Tenant
		Agents   []types.AgentApp       `json:"agents"`
		Bindings []types.ChannelBinding `json:"bindings"`
		Usage    *store.UsageSummary    `json:"usage"`
	}

	out := make([]tenantView, 0, len(tenants))
	for _, t := range tenants {
		v := tenantView{Tenant: t}

		if agents, err := c.store.ListAgentApps(reqCtx, t.TenantID); err == nil {
			v.Agents = agents
		}
		if bindings, err := c.store.ListChannelBindings(reqCtx, t.TenantID); err == nil {
			// The secret reference is a name, not a credential, but there is
			// no reason for a browser to hold it.
			for i := range bindings {
				bindings[i].SecretRef = ""
			}
			v.Bindings = bindings
		}
		if usage, err := c.store.UsageSummary(reqCtx, t.TenantID, dayAgo()); err == nil {
			v.Usage = usage
		}
		out = append(out, v)
	}
	ctx.JSON(http.StatusOK, gin.H{"tenants": out})
}

func (c *Console) fail(ctx *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, store.ErrNotFound) {
		ctx.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return true
	}
	// Scrubbed: a database error returned verbatim is exactly the kind of
	// text that carries a connection string.
	c.log.Error("console request failed",
		"path", ctx.Request.URL.Path, "error", applog.Scrub(err.Error()))
	ctx.JSON(http.StatusInternalServerError, gin.H{"error": "internal"})
	return true
}
