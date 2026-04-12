package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"

	"pulsestream/internal/deadletter"
	"pulsestream/internal/events"
	"pulsestream/internal/store"
	"pulsestream/internal/telemetry"
)

type messageReader interface {
	FetchMessage(ctx context.Context) (kafka.Message, error)
	CommitMessages(ctx context.Context, msgs ...kafka.Message) error
	Stats() kafka.ReaderStats
}

type eventStore interface {
	RecordProcessedEvent(ctx context.Context, event events.TelemetryEvent) (bool, error)
}

type deadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, record deadletter.Record) error
}

type RunnerConfig struct {
	PartitionQueueCapacity int
	Brokers                []string
	ConsumerGroup          string
	RetryBackoff           time.Duration
}

type Runner struct {
	reader       messageReader
	store        eventStore
	dlqPublisher deadLetterPublisher
	logger       *slog.Logger
	config       RunnerConfig

	startedAt time.Time
	latencies *telemetry.QuantileWindow

	processedTotal   atomic.Int64
	duplicateTotal   atomic.Int64
	deadLetterTotal  atomic.Int64
	consumerLag      atomic.Int64
	inFlightTotal    atomic.Int64
	activePartitions atomic.Int64

	commitMu sync.Mutex

	processedCounter      prometheus.Counter
	duplicateCounter      prometheus.Counter
	deadLetterCounter     *prometheus.CounterVec
	dlqPublishFailures    prometheus.Counter
	fetchErrorCounter     prometheus.Counter
	commitRetryCounter    prometheus.Counter
	processDuration       prometheus.Histogram
	consumerLagGauge      prometheus.Gauge
	inFlightGauge         prometheus.Gauge
	activePartitionsGauge prometheus.Gauge
}

func NewRunner(
	reader messageReader,
	store eventStore,
	dlqPublisher deadLetterPublisher,
	logger *slog.Logger,
	registry *prometheus.Registry,
	config RunnerConfig,
) *Runner {
	if config.PartitionQueueCapacity <= 0 {
		config.PartitionQueueCapacity = 256
	}
	if config.RetryBackoff <= 0 {
		config.RetryBackoff = time.Second
	}

	runner := &Runner{
		reader:       reader,
		store:        store,
		dlqPublisher: dlqPublisher,
		logger:       logger,
		config:       config,
		startedAt:    time.Now().UTC(),
		latencies:    telemetry.NewQuantileWindow(4096),
		processedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_processed_total",
			Help: "Number of processed events written to the hot store.",
		}),
		duplicateCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_duplicate_total",
			Help: "Number of duplicate events discarded by the processor.",
		}),
		deadLetterCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsestream_processor_dead_letter_total",
			Help: "Number of poison messages written to the dead-letter topic.",
		}, []string{"reason"}),
		dlqPublishFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_dead_letter_publish_failures_total",
			Help: "Number of dead-letter publish attempts that failed.",
		}),
		fetchErrorCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_fetch_errors_total",
			Help: "Number of Kafka fetch attempts that failed and were retried.",
		}),
		commitRetryCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_commit_retries_total",
			Help: "Number of Kafka commit attempts that failed and were retried.",
		}),
		processDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_processor_duration_seconds",
			Help:    "Processing duration per event.",
			Buckets: prometheus.DefBuckets,
		}),
		consumerLagGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsestream_processor_consumer_lag",
			Help: "Latest observed Kafka consumer lag.",
		}),
		inFlightGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsestream_processor_inflight_messages",
			Help: "Number of fetched Kafka messages that are waiting for processing or commit.",
		}),
		activePartitionsGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsestream_processor_active_partitions",
			Help: "Number of Kafka partitions currently being processed in parallel by this runner.",
		}),
	}

	registry.MustRegister(
		runner.processedCounter,
		runner.duplicateCounter,
		runner.deadLetterCounter,
		runner.dlqPublishFailures,
		runner.fetchErrorCounter,
		runner.commitRetryCounter,
		runner.processDuration,
		runner.consumerLagGauge,
		runner.inFlightGauge,
		runner.activePartitionsGauge,
	)

	return runner
}

func (r *Runner) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	partitionQueues := make(map[int]chan kafka.Message)
	var workerWG sync.WaitGroup
	errCh := make(chan error, 1)

	reportErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
		cancel()
	}

	startWorker := func(partition int, messages <-chan kafka.Message) {
		r.addActivePartitions(1)
		workerWG.Add(1)
		go func() {
			defer workerWG.Done()
			defer r.addActivePartitions(-1)

			for message := range messages {
				if err := r.handleMessage(runCtx, message); err != nil {
					reportErr(err)
					return
				}
			}
		}()
	}

	var fetchErr error
	for {
		select {
		case err := <-errCh:
			fetchErr = err
			goto shutdown
		default:
		}

		message, err := r.reader.FetchMessage(runCtx)
		if err != nil {
			if ctx.Err() != nil || runCtx.Err() != nil {
				break
			}
			r.fetchErrorCounter.Inc()
			r.logger.Warn("fetch_message_failed_retrying", "error", err, "backoff", r.config.RetryBackoff.String())
			if !sleepWithContext(runCtx, r.config.RetryBackoff) {
				break
			}
			continue
		}

		lag := r.reader.Stats().Lag
		r.consumerLag.Store(lag)
		r.consumerLagGauge.Set(float64(lag))

		queue, ok := partitionQueues[message.Partition]
		if !ok {
			queue = make(chan kafka.Message, r.config.PartitionQueueCapacity)
			partitionQueues[message.Partition] = queue
			startWorker(message.Partition, queue)
		}

		r.addInFlight(1)
		select {
		case queue <- message:
		case <-runCtx.Done():
			r.addInFlight(-1)
			break
		}
	}

shutdown:
	for _, queue := range partitionQueues {
		close(queue)
	}
	workerWG.Wait()

	select {
	case err := <-errCh:
		if fetchErr == nil {
			fetchErr = err
		}
	default:
	}

	if fetchErr != nil {
		return fetchErr
	}
	if ctx.Err() != nil {
		return nil
	}
	return nil
}

func (r *Runner) Snapshot() store.ProcessorState {
	quantiles := r.latencies.Snapshot()
	return store.ProcessorState{
		ProcessedTotal:   r.processedTotal.Load(),
		DuplicateTotal:   r.duplicateTotal.Load(),
		DeadLetterTotal:  r.deadLetterTotal.Load(),
		ConsumerLag:      r.consumerLag.Load(),
		ActivePartitions: r.activePartitions.Load(),
		InFlightMessages: r.inFlightTotal.Load(),
		LastSeenAt:       time.Now().UTC(),
		UptimeSeconds:    int64(time.Since(r.startedAt).Seconds()),
		ProcessingP50MS:  quantiles.P50,
		ProcessingP95MS:  quantiles.P95,
		ProcessingP99MS:  quantiles.P99,
	}
}

func (r *Runner) handleMessage(ctx context.Context, message kafka.Message) error {
	defer r.addInFlight(-1)

	if ctx.Err() != nil {
		return nil
	}

	ctx, span := telemetry.StartKafkaProcessSpan(ctx, message, r.config.ConsumerGroup, r.config.Brokers)
	defer span.End()

	event, reason, err := decode(message.Value)
	if err != nil {
		telemetry.SetKafkaMessageID(span, event.EventID)
		telemetry.RecordSpanError(span, err)
		if err := r.deadLetterMessage(ctx, message, event, reason, err); err != nil {
			telemetry.RecordSpanError(span, err)
			return err
		}
		return nil
	}
	telemetry.SetKafkaMessageID(span, event.EventID)

	start := time.Now()
	recorded, err := r.store.RecordProcessedEvent(ctx, event)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		telemetry.RecordSpanError(span, err)
		return fmt.Errorf("record processed event: %w", err)
	}

	if !recorded {
		r.duplicateCounter.Inc()
		r.duplicateTotal.Add(1)
	} else {
		duration := time.Since(start)
		r.processedCounter.Inc()
		r.processedTotal.Add(1)
		r.processDuration.Observe(duration.Seconds())
		r.latencies.Observe(duration)
	}

	if err := r.commitMessage(ctx, message); err != nil {
		telemetry.RecordSpanError(span, err)
		return fmt.Errorf("commit message: %w", err)
	}
	return nil
}

func (r *Runner) deadLetterMessage(
	ctx context.Context,
	message kafka.Message,
	event events.TelemetryEvent,
	reason string,
	processErr error,
) error {
	if reason == "" {
		reason = "invalid_message"
	}
	if r.dlqPublisher == nil {
		return fmt.Errorf("dead-letter publisher is not configured for %s", reason)
	}

	record := deadletter.NewRecord(
		message,
		r.config.ConsumerGroup,
		reason,
		processErr,
		event.EventID,
		event.TenantID,
		event.SourceID,
	)
	if err := r.dlqPublisher.PublishDeadLetter(ctx, record); err != nil {
		r.dlqPublishFailures.Inc()
		return fmt.Errorf("publish dead-letter message: %w", err)
	}

	r.deadLetterCounter.WithLabelValues(reason).Inc()
	r.deadLetterTotal.Add(1)
	r.logger.Warn(
		"message_dead_lettered",
		"reason", reason,
		"error", processErr,
		"topic", message.Topic,
		"partition", message.Partition,
		"offset", message.Offset,
		"tenant_id", event.TenantID,
		"source_id", event.SourceID,
		"event_id", event.EventID,
	)

	if err := r.commitMessage(ctx, message); err != nil {
		return fmt.Errorf("commit dead-lettered message: %w", err)
	}
	return nil
}

func (r *Runner) commitMessage(ctx context.Context, message kafka.Message) error {
	for {
		commitCtx, span := telemetry.StartKafkaCommitSpan(ctx, message, r.config.ConsumerGroup, r.config.Brokers)

		r.commitMu.Lock()
		err := r.reader.CommitMessages(commitCtx, message)
		r.commitMu.Unlock()

		if err == nil {
			span.End()
			return nil
		}
		if ctx.Err() != nil {
			span.End()
			return nil
		}

		telemetry.RecordSpanError(span, err)
		span.End()
		r.commitRetryCounter.Inc()
		r.logger.Warn(
			"commit_message_failed_retrying",
			"error", err,
			"topic", message.Topic,
			"partition", message.Partition,
			"offset", message.Offset,
			"backoff", r.config.RetryBackoff.String(),
		)
		if !sleepWithContext(ctx, r.config.RetryBackoff) {
			return nil
		}
	}
}

func (r *Runner) addInFlight(delta int64) {
	total := r.inFlightTotal.Add(delta)
	r.inFlightGauge.Set(float64(total))
}

func (r *Runner) addActivePartitions(delta int64) {
	total := r.activePartitions.Add(delta)
	r.activePartitionsGauge.Set(float64(total))
}

func decode(payload []byte) (events.TelemetryEvent, string, error) {
	event, err := events.DecodeTelemetryEvent(payload)
	if err == nil {
		if validationErr := event.Validate(); validationErr != nil {
			return event, "validation_failed", validationErr
		}
		return event, "", nil
	}

	var partial events.TelemetryEvent
	if unmarshalErr := json.Unmarshal(payload, &partial); unmarshalErr == nil {
		if validationErr := partial.Validate(); validationErr != nil {
			return partial, "validation_failed", validationErr
		}
	}

	return partial, "decode_failed", err
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
