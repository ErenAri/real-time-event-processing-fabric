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

	"pulsestream/internal/platform"
	"pulsestream/internal/processor"
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

	logger := platform.NewLogger("stream-processor")
	shutdownTracing, err := telemetry.Configure(ctx, "stream-processor", logger)
	if err != nil {
		return err
	}
	defer shutdownTracing(context.Background())

	listenAddr := platform.EnvString("PROCESSOR_LISTEN_ADDR", ":8082")
	brokers := platform.EnvCSV("KAFKA_BROKERS", []string{"localhost:9092"})
	topic := platform.EnvString("KAFKA_TOPIC", "pulsestream.events")
	dlqTopic := platform.EnvString("KAFKA_DLQ_TOPIC", "")
	groupID := platform.EnvString("KAFKA_GROUP_ID", "pulsestream-processor")
	postgresURL := platform.EnvString("POSTGRES_URL", "postgres://postgres:postgres@localhost:5432/pulsestream?sslmode=disable")
	postgresAdminURL := platform.EnvString("POSTGRES_ADMIN_URL", "")
	instanceID := platform.EnvInstanceID("SERVICE_INSTANCE_ID", "stream-processor")
	partitionQueueCapacity, err := platform.EnvInt("PROCESSOR_PARTITION_QUEUE_CAPACITY", 256)
	if err != nil {
		return err
	}
	retryBackoff, err := platform.EnvDuration("PROCESSOR_RETRY_BACKOFF", time.Second)
	if err != nil {
		return err
	}
	snapshotInterval, err := platform.EnvDuration("PROCESSOR_SNAPSHOT_INTERVAL", 5*time.Second)
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

	storage, err := store.NewWithAdmin(ctx, postgresURL, postgresAdminURL)
	if err != nil {
		return err
	}
	defer storage.Close()

	reader, err := platform.NewKafkaReader(brokers, topic, groupID, kafkaConnectionConfig)
	if err != nil {
		return err
	}
	defer reader.Close()

	var dlqPublisher platform.DeadLetterPublisher
	if dlqTopic != "" {
		dlqPublisher, err = platform.NewKafkaDeadLetterPublisher(brokers, dlqTopic, platform.KafkaPublisherConfig{
			ReadTimeout:  kafkaReadTimeout,
			WriteTimeout: kafkaWriteTimeout,
			MaxAttempts:  kafkaWriteMaxAttempts,
		}, kafkaConnectionConfig)
		if err != nil {
			return err
		}
		defer dlqPublisher.Close()
	}

	registry := prometheus.NewRegistry()
	registry.MustRegister(prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))

	runner := processor.NewRunner(reader, storage, dlqPublisher, logger, registry, processor.RunnerConfig{
		PartitionQueueCapacity: partitionQueueCapacity,
		Brokers:                brokers,
		ConsumerGroup:          groupID,
		RetryBackoff:           retryBackoff,
	})
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
				telemetry.InstrumentHTTP("processor-http", mux),
			),
		),
		ReadHeaderTimeout: 5 * time.Second,
	}

	processorErr := make(chan error, 1)
	go func() {
		processorErr <- runner.Run(ctx)
	}()

	go platform.RunPeriodic(ctx, snapshotInterval, logger, "processor-state-snapshot", func(runCtx context.Context) error {
		return storage.UpdateServiceState(runCtx, "stream-processor", instanceID, runner.Snapshot())
	})

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			processorErr <- fmt.Errorf("processor http server: %w", err)
		}
	}()

	logger.Info(
		"service_starting",
		"listen_addr", listenAddr,
		"kafka_topic", topic,
		"kafka_dlq_topic", dlqTopic,
		"kafka_security_protocol", kafkaConnectionConfig.SecurityProtocol,
		"kafka_sasl_mechanism", kafkaConnectionConfig.SASLMechanism,
		"group_id", groupID,
		"instance_id", instanceID,
		"partition_queue_capacity", partitionQueueCapacity,
		"retry_backoff", retryBackoff.String(),
	)
	select {
	case <-ctx.Done():
		return nil
	case err := <-processorErr:
		if err == nil {
			return nil
		}
		return err
	}
}
