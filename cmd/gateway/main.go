// Command gateway is the inbound half of the platform. It receives IM
// callbacks, deduplicates them, and queues work for Workers.
//
// It does not execute agents and never calls a Worker: the two processes
// collaborate only through the queue and shared storage.
//
// 设计依据：docs/dev/代码组织方案.md §1「仓库与进程」
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/liuzengh/trpc-agent-service/trpcservice"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels"
	"github.com/liuzengh/trpc-agent-service/trpcservice/channels/mock"
	"github.com/liuzengh/trpc-agent-service/trpcservice/config"
	"github.com/liuzengh/trpc-agent-service/trpcservice/gateway"
	applog "github.com/liuzengh/trpc-agent-service/trpcservice/log"
	"github.com/liuzengh/trpc-agent-service/trpcservice/scheduler"
	"github.com/liuzengh/trpc-agent-service/trpcservice/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "gateway: %v\n", applog.Scrub(err.Error()))
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "configs/config.yaml", "path to the configuration file")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("trpc-agent-gateway %s\n", trpcservice.Version)
		return nil
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		return err
	}

	logger := applog.Setup(cfg.Log.Level, cfg.Log.Format)
	logger.Info("gateway starting", "version", trpcservice.Version, "addr", cfg.Gateway.Addr)

	// Signals are wired before any connection so a Ctrl-C during startup is
	// still honoured rather than leaving a half-open pool behind.
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

	registry := channels.NewRegistry()
	registry.Register(mock.Name, mock.New(cfg.ResolveSecret))
	logger.Info("channels registered", "channels", registry.Names())

	gw, err := gateway.New(gateway.Deps{
		Config:     cfg,
		Store:      db,
		Dispatcher: sched,
		Mailbox:    sched,
		Channels:   registry,
		Logger:     logger,
	})
	if err != nil {
		return err
	}

	srv := &http.Server{
		Addr:              cfg.Gateway.Addr,
		Handler:           gw.Router(),
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("listening", "addr", cfg.Gateway.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	}

	// Stop accepting new callbacks, then let in-flight ones finish. A callback
	// cut off between its idempotency write and its enqueue would leave a row
	// in processing for the sweep to recover, so draining is worth the wait.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.Gateway.ShutdownTimeout)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	logger.Info("gateway stopped")
	return nil
}
