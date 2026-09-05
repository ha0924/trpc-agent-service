// Command worker consumes queued sessions and executes agents.
//
// It never receives HTTP callbacks and never calls Gateway: the two processes
// collaborate only through the queue and shared storage. Workers are
// interchangeable — any of them can serve any session — so scaling out means
// starting more of this binary.
//
// 设计依据：docs/dev/代码组织方案.md §1「仓库与进程」
//
//	docs/多租户与节点部署设计.md §7「水平扩缩容」
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	platformagent "github.com/liuzengh/trpc-agent-service/trpcservice/agent"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/storage"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
	platformtool "github.com/liuzengh/trpc-agent-service/trpcservice/tool"
	"github.com/liuzengh/trpc-agent-service/trpcservice/types"
	"github.com/liuzengh/trpc-agent-service/trpcservice/worker"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "worker: %v\n", applog.Scrub(err.Error()))
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/config.yaml", "path to the configuration file")
	healthAddr := flag.String("health-addr", ":8081", "address for the health endpoint")
	replyURL := flag.String("reply-url", "", "where the mock channel posts replies")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("trpc-agent-worker %s\n", trpcservice.Version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	logger := applog.Setup(cfg.Log.Level, cfg.Log.Format)
	logger.Info("worker starting", "version", trpcservice.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startupCtx, cancelStartup := context.WithTimeout(ctx, 15*time.Second)
	defer cancelStartup()

	db, err := store.Open(startupCtx, cfg.MySQL)
	if err != nil {
		return err
	}
	defer db.Close()
	logger.Info("mysql connected")

	sched, err := scheduler.New(startupCtx, cfg.Redis, cfg.Scheduler)
	if err != nil {
		return err
	}
	defer sched.Close()
	logger.Info("redis connected", "queue_key", cfg.Scheduler.QueueKey)

	router, err := storage.New(startupCtx, cfg, logger)
	if err != nil {
		return err
	}
	defer router.Close()

	tools := platformtool.NewRegistry()
	extensions := platformagent.NewExtensionRegistry()
	logger.Info("capabilities registered",
		"tools", tools.Names(), "extensions", extensions.Names())

	// The store is the spec loader: assembly reads a version's prompt, model
	// and bindings straight from the control plane, so no agent is defined in
	// code.
	runtimes, err := platformagent.NewProvider(platformagent.Deps{
		Config:     cfg,
		Specs:      specLoader{db},
		Router:     router,
		Tools:      tools,
		Extensions: extensions,
		Logger:     logger,
	})
	if err != nil {
		return err
	}
	defer runtimes.Close()

	registry := channels.NewRegistry()
	registry.Register(mock.Name, mock.New(cfg.ResolveSecret, mock.WithReplyURL(*replyURL)))
	logger.Info("channels registered", "channels", registry.Names(), "reply_url", *replyURL)

	w, err := worker.New(worker.Deps{
		Config:     cfg,
		Store:      db,
		Dispatcher: sched,
		Mailbox:    sched,
		Lease:      sched,
		Runtimes:   runtimes,
		Channels:   registry,
		Logger:     logger,
	})
	if err != nil {
		return err
	}

	// A health endpoint, not a service: Workers take work from the queue and
	// expose nothing callable.
	health := startHealthServer(*healthAddr, db, sched, runtimes, logger)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		health.Shutdown(shutdownCtx)
	}()

	go evictIdleRuntimes(ctx, runtimes, cfg.Runtime.IdleTTL, logger)

	logger.Info("worker ready", "worker_id", w.ID())
	if err := w.Run(ctx); err != nil {
		return err
	}
	logger.Info("worker stopped")
	return nil
}

// specLoader adapts the store to the provider's loader interface, keeping the
// agent package free of any database dependency.
type specLoader struct{ *store.Store }

// Load reads the version configuration the assembler needs.
func (l specLoader) Load(ctx context.Context, key types.RuntimeKey) (*types.RuntimeSpec, error) {
	return l.Store.RuntimeSpec(ctx, key)
}

// evictIdleRuntimes releases Runtimes no session has used recently. Without
// it, a Worker that once served many tenants would hold every model client
// for the life of the process.
func evictIdleRuntimes(ctx context.Context, p *platformagent.Provider, idleTTL time.Duration, logger *slog.Logger) {
	if idleTTL <= 0 {
		return
	}
	interval := idleTTL / 2
	if interval < time.Minute {
		interval = time.Minute
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if n := p.EvictIdle(); n > 0 {
				logger.Info("idle runtimes evicted", "count", n, "cached", p.Len())
			}
		}
	}
}

func startHealthServer(
	addr string,
	db *store.Store,
	sched *scheduler.Redis,
	runtimes *platformagent.Provider,
	logger *slog.Logger,
) *http.Server {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := db.Ping(ctx); err != nil {
			http.Error(w, "mysql unavailable", http.StatusServiceUnavailable)
			return
		}
		if err := sched.Ping(ctx); err != nil {
			http.Error(w, "redis unavailable", http.StatusServiceUnavailable)
			return
		}
		queued, _ := sched.QueueLen(ctx)
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","cached_runtimes":%d,"queued_hints":%d}`,
			runtimes.Len(), queued)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("health server stopped", "error", err.Error())
		}
	}()
	logger.Info("health endpoint listening", "addr", addr)
	return srv
}
