package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"jobsched/internal/clock"
	"jobsched/internal/deps"
	"jobsched/internal/dispatch"
	"jobsched/internal/exec"
	"jobsched/internal/flow"
	"jobsched/internal/heartbeat"
	"jobsched/internal/lease"
	"jobsched/internal/metric"
	"jobsched/internal/model"
	"jobsched/internal/publish"
	"jobsched/internal/queue"
	"jobsched/internal/registry"
	"jobsched/internal/store"
)

func main() {
	addr := flag.String("addr", ":8080", "listen address")
	flag.Parse()

	cfg := Load(*addr)
	st := store.NewStore(cfg.StoreCapacity)
	q := queue.New(cfg.QueueSize)
	reg := registry.New()
	dg := deps.New()
	metrics := metric.New()

	handler := func(inst model.Instance) error {
		log.Printf("executing task %s instance %s", inst.TaskID, inst.ID)
		st.RecordTaskExecution(inst)
		metrics.Succeeded()
		return nil
	}
	execs := exec.NewManager(handler, cfg.RunnerQueue, cfg.RunnerTimeout)
	leases := lease.New(cfg.LeaseTimeout)
	limiter := flow.NewLimiter(cfg.Flow)
	dispatcher := dispatch.New(q, st, reg, leases, execs, metrics, limiter)
	broker := publish.New(st, q, reg, dg, metrics)
	heart := heartbeat.New(leases, execs, metrics, cfg.HeartbeatEvery, cfg.LeaseTimeout)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go heart.Run(ctx)
	go func() {
		ticker := time.NewTicker(cfg.TickInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				win := clock.NextWindow(now, cfg.TickInterval)
				if err := dispatcher.Tick(ctx, win); err != nil {
					log.Printf("tick failed: %v", err)
				}
			}
		}
	}()

	srv := NewServer(cfg, reg, dg, leases, execs, metrics, broker, dispatcher, st)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe(cfg.Addr)
	}()

	if err := probeWithRetry("http://"+cfg.Addr, 20); err != nil {
		log.Printf("startup probe failed: %v", err)
	} else {
		log.Printf("scheduler listening on %s", cfg.Addr)
	}

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			log.Printf("shutdown failed: %v", err)
		}
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("server failed: %v", err)
		}
	}
}
