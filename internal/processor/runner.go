package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
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
	RecordProcessedEventBatch(ctx context.Context, inputs []store.ProcessedEventInput) (store.ProcessedEventBatchResult, error)
}

type deadLetterPublisher interface {
	PublishDeadLetter(ctx context.Context, record deadletter.Record) error
}

type RunnerConfig struct {
	PartitionQueueCapacity int
	Brokers                []string
	ConsumerGroup          string
	RetryBackoff           time.Duration
	BatchFlushInterval     time.Duration
	MaxBatchSize           int
	AllowedLateness        time.Duration
}

type batchMessage struct {
	Message kafka.Message
	Event   events.TelemetryEvent
	Late    bool
}

type partitionRuntimeState struct {
	Lag              int64
	ProcessedTotal   int64
	DuplicateTotal   int64
	LateEventTotal   int64
	InFlightMessages int64
	LastOffset       int64
	LastSeenAt       time.Time
}

type Runner struct {
	reader       messageReader
	store        eventStore
	dlqPublisher deadLetterPublisher
	logger       *slog.Logger
	config       RunnerConfig

	startedAt      time.Time
	latencies      *telemetry.QuantileWindow
	batchLatencies *telemetry.QuantileWindow
	batchSizes     *telemetry.QuantileWindow

	processedTotal   atomic.Int64
	duplicateTotal   atomic.Int64
	lateEventTotal   atomic.Int64
	deadLetterTotal  atomic.Int64
	consumerLag      atomic.Int64
	inFlightTotal    atomic.Int64
	activePartitions atomic.Int64

	commitMu    sync.Mutex
	partitionMu sync.Mutex
	partitions  map[int]*partitionRuntimeState

	processedCounter          prometheus.Counter
	duplicateCounter          prometheus.Counter
	lateEventCounter          prometheus.Counter
	deadLetterCounter         *prometheus.CounterVec
	dlqPublishFailures        prometheus.Counter
	fetchErrorCounter         prometheus.Counter
	commitRetryCounter        prometheus.Counter
	processDuration           prometheus.Histogram
	stageDuration             *prometheus.HistogramVec
	batchFlushDuration        prometheus.Histogram
	batchSize                 prometheus.Histogram
	consumerLagGauge          prometheus.Gauge
	inFlightGauge             prometheus.Gauge
	activePartitionsGauge     prometheus.Gauge
	partitionProcessedCounter *prometheus.CounterVec
	partitionLagGauge         *prometheus.GaugeVec
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
	if config.BatchFlushInterval <= 0 {
		config.BatchFlushInterval = 100 * time.Millisecond
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 500
	}
	if config.AllowedLateness <= 0 {
		config.AllowedLateness = 2 * time.Minute
	}

	runner := &Runner{
		reader:         reader,
		store:          store,
		dlqPublisher:   dlqPublisher,
		logger:         logger,
		config:         config,
		startedAt:      time.Now().UTC(),
		latencies:      telemetry.NewQuantileWindow(4096),
		batchLatencies: telemetry.NewQuantileWindow(2048),
		batchSizes:     telemetry.NewQuantileWindow(2048),
		partitions:     map[int]*partitionRuntimeState{},
		processedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_processed_total",
			Help: "Number of processed events written to the hot store.",
		}),
		duplicateCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_duplicate_total",
			Help: "Number of duplicate events discarded by the processor.",
		}),
		lateEventCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_late_event_total",
			Help: "Number of newly claimed events skipped from event-time windows because they exceeded allowed lateness.",
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
		stageDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "pulsestream_processor_stage_duration_seconds",
			Help:    "Duration of processor store stages inside a successful batch.",
			Buckets: prometheus.DefBuckets,
		}, []string{"stage"}),
		batchFlushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_processor_batch_flush_duration_seconds",
			Help:    "End-to-end duration of processor batch store write and Kafka offset commit.",
			Buckets: prometheus.DefBuckets,
		}),
		batchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_processor_batch_size",
			Help:    "Number of Kafka messages included in each processor batch.",
			Buckets: []float64{1, 5, 10, 25, 50, 100, 250, 500, 1000},
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
		partitionProcessedCounter: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "pulsestream_processor_partition_processed_total",
			Help: "Number of newly processed events by Kafka partition.",
		}, []string{"partition"}),
		partitionLagGauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "pulsestream_processor_partition_lag",
			Help: "Latest observed lag for Kafka partitions owned by this processor.",
		}, []string{"partition"}),
	}

	registry.MustRegister(
		runner.processedCounter,
		runner.duplicateCounter,
		runner.lateEventCounter,
		runner.deadLetterCounter,
		runner.dlqPublishFailures,
		runner.fetchErrorCounter,
		runner.commitRetryCounter,
		runner.processDuration,
		runner.stageDuration,
		runner.batchFlushDuration,
		runner.batchSize,
		runner.consumerLagGauge,
		runner.inFlightGauge,
		runner.activePartitionsGauge,
		runner.partitionProcessedCounter,
		runner.partitionLagGauge,
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

			if err := r.runPartitionWorker(runCtx, partition, messages); err != nil {
				reportErr(err)
				return
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
		r.markPartitionFetched(message.Partition, message.Offset, lag)
		select {
		case queue <- message:
		case <-runCtx.Done():
			r.addInFlight(-1)
			r.markPartitionReleased(message.Partition, 1)
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
	batchFlushQuantiles := r.batchLatencies.Snapshot()
	batchSizeQuantiles := r.batchSizes.Snapshot()
	return store.ProcessorState{
		ProcessedTotal:   r.processedTotal.Load(),
		DuplicateTotal:   r.duplicateTotal.Load(),
		LateEventTotal:   r.lateEventTotal.Load(),
		DeadLetterTotal:  r.deadLetterTotal.Load(),
		ConsumerLag:      r.consumerLag.Load(),
		ActivePartitions: r.activePartitions.Load(),
		InFlightMessages: r.inFlightTotal.Load(),
		LastSeenAt:       time.Now().UTC(),
		UptimeSeconds:    int64(time.Since(r.startedAt).Seconds()),
		ProcessingP50MS:  quantiles.P50,
		ProcessingP95MS:  quantiles.P95,
		ProcessingP99MS:  quantiles.P99,
		BatchSizeP50:     batchSizeQuantiles.P50,
		BatchSizeP95:     batchSizeQuantiles.P95,
		BatchSizeP99:     batchSizeQuantiles.P99,
		BatchFlushP50MS:  batchFlushQuantiles.P50,
		BatchFlushP95MS:  batchFlushQuantiles.P95,
		BatchFlushP99MS:  batchFlushQuantiles.P99,
		Partitions:       r.partitionSnapshot(),
	}
}

func (r *Runner) runPartitionWorker(ctx context.Context, partition int, messages <-chan kafka.Message) error {
	buffer := make([]batchMessage, 0, r.config.MaxBatchSize)
	timer := time.NewTimer(r.config.BatchFlushInterval)
	defer timer.Stop()

	var maxEventTime time.Time
	resetTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(r.config.BatchFlushInterval)
	}
	flush := func() error {
		if len(buffer) == 0 {
			resetTimer()
			return nil
		}
		pending := buffer
		buffer = make([]batchMessage, 0, r.config.MaxBatchSize)
		if err := r.flushBatch(ctx, partition, pending); err != nil {
			return err
		}
		resetTimer()
		return nil
	}

	for {
		select {
		case message, ok := <-messages:
			if !ok {
				return flush()
			}
			if ctx.Err() != nil {
				r.addInFlight(-1)
				r.markPartitionReleased(partition, 1)
				return nil
			}

			event, reason, err := decode(message.Value)
			if err != nil {
				if err := r.deadLetterMessage(ctx, message, event, reason, err); err != nil {
					r.addInFlight(-1)
					r.markPartitionReleased(partition, 1)
					return err
				}
				r.addInFlight(-1)
				r.markPartitionReleased(partition, 1)
				continue
			}

			eventTime := event.Timestamp.UTC()
			if maxEventTime.IsZero() || eventTime.After(maxEventTime) {
				maxEventTime = eventTime
			}
			late := eventTime.Before(maxEventTime.Add(-r.config.AllowedLateness))
			buffer = append(buffer, batchMessage{
				Message: message,
				Event:   event,
				Late:    late,
			})
			if len(buffer) >= r.config.MaxBatchSize {
				if err := flush(); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := flush(); err != nil {
				return err
			}
		case <-ctx.Done():
			for range buffer {
				r.addInFlight(-1)
				r.markPartitionReleased(partition, 1)
			}
			return nil
		}
	}
}

func (r *Runner) flushBatch(ctx context.Context, partition int, items []batchMessage) error {
	if len(items) == 0 {
		return nil
	}
	released := false
	defer func() {
		if !released {
			r.addInFlight(-int64(len(items)))
			r.markPartitionReleased(partition, len(items))
		}
	}()

	inputs := make([]store.ProcessedEventInput, 0, len(items))
	messages := make([]kafka.Message, 0, len(items))
	for _, item := range items {
		inputs = append(inputs, store.ProcessedEventInput{
			Event: item.Event,
			Late:  item.Late,
		})
		messages = append(messages, item.Message)
	}

	start := time.Now()
	result, err := r.store.RecordProcessedEventBatch(ctx, inputs)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return fmt.Errorf("record processed event batch: %w", err)
	}

	if result.RecordedCount > 0 {
		r.processedCounter.Add(float64(result.RecordedCount))
		r.processedTotal.Add(result.RecordedCount)
	}
	if result.DuplicateCount > 0 {
		r.duplicateCounter.Add(float64(result.DuplicateCount))
		r.duplicateTotal.Add(result.DuplicateCount)
	}
	if result.LateCount > 0 {
		r.lateEventCounter.Add(float64(result.LateCount))
		r.lateEventTotal.Add(result.LateCount)
	}
	r.observeStageDurations(result.StageDurations)

	if err := r.commitMessages(ctx, messages); err != nil {
		return fmt.Errorf("commit batch messages: %w", err)
	}

	duration := time.Since(start)
	r.batchFlushDuration.Observe(duration.Seconds())
	r.batchLatencies.Observe(duration)
	r.batchSize.Observe(float64(len(items)))
	r.batchSizes.Observe(time.Duration(len(items)) * time.Millisecond)
	if result.RecordedCount > 0 {
		perRecorded := duration / time.Duration(result.RecordedCount)
		r.processDuration.Observe(perRecorded.Seconds())
		r.latencies.Observe(perRecorded)
	}
	r.partitionProcessedCounter.WithLabelValues(strconv.Itoa(partition)).Add(float64(result.RecordedCount))
	r.markPartitionFlushed(partition, len(items), result)
	r.addInFlight(-int64(len(items)))
	released = true
	return nil
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
	result, err := r.store.RecordProcessedEventBatch(ctx, []store.ProcessedEventInput{{Event: event}})
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		telemetry.RecordSpanError(span, err)
		return fmt.Errorf("record processed event: %w", err)
	}

	if result.DuplicateCount > 0 {
		r.duplicateCounter.Add(float64(result.DuplicateCount))
		r.duplicateTotal.Add(result.DuplicateCount)
	}
	if result.LateCount > 0 {
		r.lateEventCounter.Add(float64(result.LateCount))
		r.lateEventTotal.Add(result.LateCount)
	}
	if result.RecordedCount > 0 {
		duration := time.Since(start)
		r.processedCounter.Add(float64(result.RecordedCount))
		r.processedTotal.Add(result.RecordedCount)
		r.processDuration.Observe(duration.Seconds())
		r.latencies.Observe(duration)
	}
	r.observeStageDurations(result.StageDurations)

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

func (r *Runner) commitMessages(ctx context.Context, messages []kafka.Message) error {
	if len(messages) == 0 {
		return nil
	}

	first := messages[0]
	last := messages[len(messages)-1]
	for {
		start := time.Now()
		r.commitMu.Lock()
		err := r.reader.CommitMessages(ctx, messages...)
		r.commitMu.Unlock()
		r.stageDuration.WithLabelValues("kafka_offset_commit").Observe(time.Since(start).Seconds())

		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		r.commitRetryCounter.Inc()
		r.logger.Warn(
			"commit_batch_failed_retrying",
			"error", err,
			"topic", last.Topic,
			"partition", last.Partition,
			"first_offset", first.Offset,
			"last_offset", last.Offset,
			"message_count", len(messages),
			"backoff", r.config.RetryBackoff.String(),
		)
		if !sleepWithContext(ctx, r.config.RetryBackoff) {
			return nil
		}
	}
}

func (r *Runner) observeStageDurations(stages store.ProcessorStageDurations) {
	if stages.DedupClaimMS > 0 {
		r.stageDuration.WithLabelValues("dedup_claim").Observe(stages.DedupClaimMS / 1000.0)
	}
	if stages.TenantAggregateMS > 0 {
		r.stageDuration.WithLabelValues("tenant_aggregate_upsert").Observe(stages.TenantAggregateMS / 1000.0)
	}
	if stages.SourceAggregateMS > 0 {
		r.stageDuration.WithLabelValues("source_aggregate_upsert").Observe(stages.SourceAggregateMS / 1000.0)
	}
	if stages.WindowAggregateMS > 0 {
		r.stageDuration.WithLabelValues("window_aggregate_upsert").Observe(stages.WindowAggregateMS / 1000.0)
	}
	if stages.CommitMS > 0 {
		r.stageDuration.WithLabelValues("db_transaction_commit").Observe(stages.CommitMS / 1000.0)
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

func (r *Runner) markPartitionFetched(partition int, offset int64, lag int64) {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()

	state := r.partitions[partition]
	if state == nil {
		state = &partitionRuntimeState{}
		r.partitions[partition] = state
	}
	state.Lag = lag
	state.InFlightMessages++
	state.LastOffset = offset
	state.LastSeenAt = time.Now().UTC()
	r.partitionLagGauge.WithLabelValues(strconv.Itoa(partition)).Set(float64(lag))
}

func (r *Runner) markPartitionReleased(partition int, count int) {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()

	state := r.partitions[partition]
	if state == nil {
		return
	}
	state.InFlightMessages -= int64(count)
	if state.InFlightMessages < 0 {
		state.InFlightMessages = 0
	}
	state.LastSeenAt = time.Now().UTC()
}

func (r *Runner) markPartitionFlushed(partition int, count int, result store.ProcessedEventBatchResult) {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()

	state := r.partitions[partition]
	if state == nil {
		state = &partitionRuntimeState{}
		r.partitions[partition] = state
	}
	state.ProcessedTotal += result.RecordedCount
	state.DuplicateTotal += result.DuplicateCount
	state.LateEventTotal += result.LateCount
	state.InFlightMessages -= int64(count)
	if state.InFlightMessages < 0 {
		state.InFlightMessages = 0
	}
	state.LastSeenAt = time.Now().UTC()
}

func (r *Runner) partitionSnapshot() []store.PartitionState {
	r.partitionMu.Lock()
	defer r.partitionMu.Unlock()

	partitions := make([]store.PartitionState, 0, len(r.partitions))
	for partition, state := range r.partitions {
		partitions = append(partitions, store.PartitionState{
			Partition:        partition,
			Lag:              state.Lag,
			ProcessedTotal:   state.ProcessedTotal,
			DuplicateTotal:   state.DuplicateTotal,
			LateEventTotal:   state.LateEventTotal,
			InFlightMessages: state.InFlightMessages,
			LastOffset:       state.LastOffset,
			LastSeenAt:       state.LastSeenAt,
		})
	}
	sort.Slice(partitions, func(i, j int) bool {
		return partitions[i].Partition < partitions[j].Partition
	})
	return partitions
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
