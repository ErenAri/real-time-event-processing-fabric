package main

import (
	"context"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pulsestream/internal/platform"
	"pulsestream/internal/simulator"
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

	logger := platform.NewLogger("producer-simulator")
	shutdownTracing, err := telemetry.Configure(ctx, "producer-simulator", logger)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	listenAddr := platform.EnvString("SIMULATOR_LISTEN_ADDR", ":8083")
	ratePerSecond, err := platform.EnvInt("SIM_RATE_PER_SEC", 200)
	if err != nil {
		return err
	}
	tenantCount, err := platform.EnvInt("SIM_TENANT_COUNT", 5)
	if err != nil {
		return err
	}
	sourcesPerTenant, err := platform.EnvInt("SIM_SOURCES_PER_TENANT", 25)
	if err != nil {
		return err
	}
	defaultMaxInFlight := ratePerSecond
	if defaultMaxInFlight < 64 {
		defaultMaxInFlight = 64
	}
	if defaultMaxInFlight > 2048 {
		defaultMaxInFlight = 2048
	}
	maxInFlight, err := platform.EnvInt("SIM_MAX_IN_FLIGHT", defaultMaxInFlight)
	if err != nil {
		return err
	}
	duplicateEvery, err := platform.EnvInt64("SIM_DUPLICATE_EVERY", 75)
	if err != nil {
		return err
	}
	malformedEvery, err := platform.EnvInt64("SIM_MALFORMED_EVERY", 150)
	if err != nil {
		return err
	}
	burstEvery, err := platform.EnvDuration("SIM_BURST_EVERY", 30*time.Second)
	if err != nil {
		return err
	}
	burstSize, err := platform.EnvInt("SIM_BURST_SIZE", 50)
	if err != nil {
		return err
	}
	seed, err := platform.EnvInt64("SIM_SEED", 42)
	if err != nil {
		return err
	}
	endpoint := platform.EnvString("SIM_INGEST_ENDPOINT", "http://localhost:8080/api/v1/events")
	bearerToken := platform.EnvString("SIM_BEARER_TOKEN", "")
	producerID := platform.EnvString("SIM_PRODUCER_ID", "")

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	generator := simulator.NewGenerator(simulator.Config{
		Endpoint:         endpoint,
		BearerToken:      bearerToken,
		ProducerID:       producerID,
		RatePerSecond:    ratePerSecond,
		TenantCount:      tenantCount,
		SourcesPerTenant: sourcesPerTenant,
		MaxInFlight:      maxInFlight,
		DuplicateEvery:   duplicateEvery,
		MalformedEvery:   malformedEvery,
		BurstEvery:       burstEvery,
		BurstSize:        burstSize,
		Seed:             seed,
	}, logger, registry)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		platform.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		platform.WriteJSON(w, http.StatusOK, map[string]string{"status": "ready"})
	})

	server := &http.Server{
		Addr: listenAddr,
		Handler: platform.WithCORS(
			platform.RequestLogging(
				logger,
				telemetry.InstrumentHTTP("simulator-http", mux),
			),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		errCh <- generator.Run(ctx)
	}()
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info("service_starting", "listen_addr", listenAddr, "ingest_endpoint", endpoint, "rate_per_second", ratePerSecond, "producer_id", producerID)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}
