package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pulsestream/internal/api"
	eventarchive "pulsestream/internal/archive"
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

	logger := platform.NewLogger("ingest-service")
	shutdownTracing, err := telemetry.Configure(ctx, "ingest-service", logger)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	listenAddr := platform.EnvString("INGEST_LISTEN_ADDR", ":8080")
	brokers := platform.EnvCSV("KAFKA_BROKERS", []string{"localhost:9092"})
	topic := platform.EnvString("KAFKA_TOPIC", "pulsestream.events")
	postgresURL := platform.EnvString("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/pulsestream?sslmode=disable")
	rawArchiveDir := platform.EnvString("RAW_ARCHIVE_DIR", "data/raw-archive")
	adminToken := platform.EnvString("ADMIN_TOKEN", "pulsestream-dev-admin")
	instanceID := platform.EnvInstanceID("SERVICE_INSTANCE_ID", "ingest-service")
	kafkaBatchTimeout, err := platform.EnvDuration("KAFKA_BATCH_TIMEOUT", 5*time.Millisecond)
	if err != nil {
		return err
	}
	kafkaBatchSize, err := platform.EnvInt("KAFKA_BATCH_SIZE", 256)
	if err != nil {
		return err
	}
	snapshotInterval, err := platform.EnvDuration("INGEST_SNAPSHOT_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}

	storage, err := store.New(ctx, postgresURL)
	if err != nil {
		return err
	}
	defer storage.Close()

	publisher := platform.NewKafkaPublisher(brokers, topic, platform.KafkaPublisherConfig{
		BatchTimeout: kafkaBatchTimeout,
		BatchSize:    kafkaBatchSize,
	})
	defer publisher.Close()
	archiver := eventarchive.NewFileArchive(rawArchiveDir)
	defer archiver.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	handler := api.NewIngestHandler(logger, publisher, storage, archiver, archiver, adminToken, registry)
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
				telemetry.InstrumentHTTP("ingest-http", mux),
			),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go platform.RunPeriodic(ctx, snapshotInterval, logger, "ingest-state-snapshot", func(runCtx context.Context) error {
		return storage.UpdateServiceState(runCtx, "ingest-service", instanceID, handler.Snapshot())
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	logger.Info(
		"service_starting",
		"listen_addr", listenAddr,
		"kafka_topic", topic,
		"kafka_brokers", fmt.Sprintf("%v", brokers),
		"kafka_batch_timeout", kafkaBatchTimeout.String(),
		"kafka_batch_size", kafkaBatchSize,
		"raw_archive_dir", rawArchiveDir,
		"instance_id", instanceID,
	)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
