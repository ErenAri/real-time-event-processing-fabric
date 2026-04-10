package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pulsestream/internal/api"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
	"pulsestream/internal/telemetry"
)

func main() {
	if err := run(); err != nil {
		panic(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	logger := platform.NewLogger("query-service")
	shutdownTracing, err := telemetry.Configure(ctx, "query-service", logger)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	listenAddr := platform.EnvString("QUERY_LISTEN_ADDR", ":8081")
	postgresURL := platform.EnvString("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/pulsestream?sslmode=disable")
	instanceID := platform.EnvInstanceID("SERVICE_INSTANCE_ID", "query-service")
	snapshotInterval, err := platform.EnvDuration("QUERY_SNAPSHOT_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}

	storage, err := store.New(ctx, postgresURL)
	if err != nil {
		return err
	}
	defer storage.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	handler := api.NewQueryHandler(logger, storage, registry)
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platform.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		platform.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	handler.RegisterRoutes(mux)

	server := &http.Server{
		Addr: listenAddr,
		Handler: platform.WithCORS(
			platform.RequestLogging(
				logger,
				telemetry.InstrumentHTTP("query-http", mux),
			),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go platform.RunPeriodic(ctx, snapshotInterval, logger, "query-state-snapshot", func(runCtx context.Context) error {
		return storage.UpdateServiceState(runCtx, "query-service", instanceID, handler.Snapshot())
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("service_starting", "listen_addr", listenAddr, "instance_id", instanceID)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
