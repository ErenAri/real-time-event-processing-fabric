package main

import (
	"context"
	"fmt"
	"net/http"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"pulsestream/internal/api"
	eventarchive "pulsestream/internal/archive"
	"pulsestream/internal/auth"
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
	postgresAdminURL := platform.EnvString("POSTGRES_ADMIN_URL", "")
	rawArchiveBackend := platform.EnvString("RAW_ARCHIVE_BACKEND", eventarchive.BackendFilesystem)
	rawArchiveDir := platform.EnvString("RAW_ARCHIVE_DIR", "data/raw-archive")
	rawArchiveFlushInterval, err := platform.EnvDuration("RAW_ARCHIVE_FLUSH_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}
	rawArchiveFlushBytes, err := platform.EnvInt("RAW_ARCHIVE_FLUSH_BYTES", 256*1024)
	if err != nil {
		return err
	}
	adminToken := platform.EnvString("ADMIN_TOKEN", "pulsestream-dev-admin")
	jwtSecret := platform.EnvString("AUTH_JWT_SECRET", "")
	jwtIssuer := platform.EnvString("AUTH_JWT_ISSUER", "")
	jwtAudience := platform.EnvString("AUTH_JWT_AUDIENCE", "")
	instanceID := platform.EnvInstanceID("SERVICE_INSTANCE_ID", "ingest-service")
	kafkaBatchTimeout, err := platform.EnvDuration("KAFKA_BATCH_TIMEOUT", 5*time.Millisecond)
	if err != nil {
		return err
	}
	kafkaBatchSize, err := platform.EnvInt("KAFKA_BATCH_SIZE", 256)
	if err != nil {
		return err
	}
	kafkaReadTimeout, err := platform.EnvDuration("KAFKA_READ_TIMEOUT", 2*time.Second)
	if err != nil {
		return err
	}
	kafkaWriteTimeout, err := platform.EnvDuration("KAFKA_WRITE_TIMEOUT", 2*time.Second)
	if err != nil {
		return err
	}
	kafkaWriteMaxAttempts, err := platform.EnvInt("KAFKA_WRITE_MAX_ATTEMPTS", 2)
	if err != nil {
		return err
	}
	kafkaConnectionConfig, err := platform.LoadKafkaConnectionConfigFromEnv()
	if err != nil {
		return err
	}
	snapshotInterval, err := platform.EnvDuration("INGEST_SNAPSHOT_INTERVAL", 5*time.Second)
	if err != nil {
		return err
	}
	maxInFlight, err := platform.EnvInt("INGEST_MAX_INFLIGHT", 256)
	if err != nil {
		return err
	}

	storage, err := store.NewWithAdmin(ctx, postgresURL, postgresAdminURL)
	if err != nil {
		return err
	}
	defer storage.Close()

	var verifier *auth.Verifier
	if strings.TrimSpace(jwtSecret) != "" {
		verifier, err = auth.NewVerifier(jwtSecret, jwtIssuer, jwtAudience)
		if err != nil {
			return err
		}
	}

	publisher, err := platform.NewKafkaPublisher(brokers, topic, platform.KafkaPublisherConfig{
		BatchTimeout: kafkaBatchTimeout,
		BatchSize:    kafkaBatchSize,
		ReadTimeout:  kafkaReadTimeout,
		WriteTimeout: kafkaWriteTimeout,
		MaxAttempts:  kafkaWriteMaxAttempts,
	}, kafkaConnectionConfig)
	if err != nil {
		return err
	}
	defer publisher.Close()
	archiver, err := eventarchive.New(ctx, eventarchive.Config{
		Backend:  rawArchiveBackend,
		FileRoot: rawArchiveDir,
		AzureBlob: eventarchive.AzureBlobConfig{
			AccountURL:       platform.EnvString("RAW_ARCHIVE_AZURE_BLOB_ACCOUNT_URL", ""),
			ConnectionString: platform.EnvString("RAW_ARCHIVE_AZURE_BLOB_CONNECTION_STRING", ""),
			Container:        platform.EnvString("RAW_ARCHIVE_AZURE_BLOB_CONTAINER", ""),
			Prefix:           platform.EnvString("RAW_ARCHIVE_AZURE_BLOB_PREFIX", "raw-archive"),
			FlushInterval:    rawArchiveFlushInterval,
			FlushBytes:       rawArchiveFlushBytes,
		},
	})
	if err != nil {
		return err
	}
	defer archiver.Close()

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	handler := api.NewIngestHandler(logger, publisher, storage, archiver, archiver, verifier, adminToken, registry)
	handler.SetMaxInFlight(maxInFlight)
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
		"kafka_security_protocol", kafkaConnectionConfig.SecurityProtocol,
		"kafka_sasl_mechanism", kafkaConnectionConfig.SASLMechanism,
		"kafka_batch_timeout", kafkaBatchTimeout.String(),
		"kafka_batch_size", kafkaBatchSize,
		"kafka_read_timeout", kafkaReadTimeout.String(),
		"kafka_write_timeout", kafkaWriteTimeout.String(),
		"kafka_write_max_attempts", kafkaWriteMaxAttempts,
		"raw_archive_backend", rawArchiveBackend,
		"raw_archive_dir", rawArchiveDir,
		"max_inflight", maxInFlight,
		"instance_id", instanceID,
	)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}
