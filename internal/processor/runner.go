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

type RunnerConfig struct {
	PartitionQueueCapacity int
}

type Runner struct {
	reader messageReader
	store  eventStore
	logger *slog.Logger
	config RunnerConfig

	startedAt time.Time
	latencies *telemetry.QuantileWindow

	processedTotal   atomic.Int64
	duplicateTotal   atomic.Int64
	consumerLag      atomic.Int64
	inFlightTotal    atomic.Int64
	activePartitions atomic.Int64

	commitMu sync.Mutex

	processedCounter      prometheus.Counter
	duplicateCounter      prometheus.Counter
	processDuration       prometheus.Histogram
	consumerLagGauge      prometheus.Gauge
	inFlightGauge         prometheus.Gauge
	activePartitionsGauge prometheus.Gauge
}

func NewRunner(
	reader messageReader,
	store eventStore,
	logger *slog.Logger,
	registry *prometheus.Registry,
	config RunnerConfig,
) *Runner {
	if config.PartitionQueueCapacity <= 0 {
		config.PartitionQueueCapacity = 256
	}

	runner := &Runner{
		reader:    reader,
		store:     store,
		logger:    logger,
		config:    config,
		startedAt: time.Now().UTC(),
		latencies: telemetry.NewQuantileWindow(4096),
		processedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_processed_total",
			Help: "Number of processed events written to the hot store.",
		}),
		duplicateCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_processor_duplicate_total",
			Help: "Number of duplicate events discarded by the processor.",
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
			fetchErr = fmt.Errorf("fetch kafka message: %w", err)
			break
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

	event, err := decode(message.Value)
	if err != nil {
		r.logger.Error("decode_message_failed", "error", err)
		if err := r.commitMessage(ctx, message); err != nil {
			return fmt.Errorf("commit malformed message: %w", err)
		}
		return nil
	}

	start := time.Now()
	recorded, err := r.store.RecordProcessedEvent(ctx, event)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
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
		return fmt.Errorf("commit message: %w", err)
	}
	return nil
}

func (r *Runner) commitMessage(ctx context.Context, message kafka.Message) error {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()

	if err := r.reader.CommitMessages(ctx, message); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	return nil
}

func (r *Runner) addInFlight(delta int64) {
	total := r.inFlightTotal.Add(delta)
	r.inFlightGauge.Set(float64(total))
}

func (r *Runner) addActivePartitions(delta int64) {
	total := r.activePartitions.Add(delta)
	r.activePartitionsGauge.Set(float64(total))
}

func decode(payload []byte) (events.TelemetryEvent, error) {
	var event events.TelemetryEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return events.TelemetryEvent{}, err
	}
	return event, event.Validate()
}
