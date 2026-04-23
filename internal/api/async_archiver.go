package api

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/events"
)

var (
	ErrArchiveQueueFull = errors.New("archive queue full")
	ErrArchiveClosed    = errors.New("archive queue closed")
)

type AsyncArchiverConfig struct {
	QueueCapacity  int
	Workers        int
	EnqueueTimeout time.Duration
	WriteTimeout   time.Duration
}

type AsyncArchiver struct {
	logger *slog.Logger
	next   RawArchiver

	queue chan archiveJob

	workers        int
	enqueueTimeout time.Duration
	writeTimeout   time.Duration

	mu        sync.RWMutex
	closed    bool
	startOnce sync.Once
	closeOnce sync.Once
	wg        sync.WaitGroup

	enqueueDuration prometheus.Histogram
	writeDuration   prometheus.Histogram
	queuedCounter   prometheus.Counter
	writtenCounter  prometheus.Counter
	failedCounter   prometheus.Counter
	fullCounter     prometheus.Counter
	depthGauge      prometheus.Gauge
}

type archiveJob struct {
	event      events.TelemetryEvent
	rawPayload []byte
}

func NewAsyncArchiver(logger *slog.Logger, next RawArchiver, registerer prometheus.Registerer, config AsyncArchiverConfig) *AsyncArchiver {
	if config.QueueCapacity <= 0 {
		config.QueueCapacity = 10000
	}
	if config.Workers <= 0 {
		config.Workers = 1
	}
	if config.EnqueueTimeout <= 0 {
		config.EnqueueTimeout = 10 * time.Millisecond
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = 30 * time.Second
	}

	archiver := &AsyncArchiver{
		logger:         logger,
		next:           next,
		queue:          make(chan archiveJob, config.QueueCapacity),
		workers:        config.Workers,
		enqueueTimeout: config.EnqueueTimeout,
		writeTimeout:   config.WriteTimeout,
		enqueueDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_ingest_archive_enqueue_duration_seconds",
			Help:    "Duration spent enqueueing valid events for asynchronous raw archive writes.",
			Buckets: prometheus.DefBuckets,
		}),
		writeDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "pulsestream_ingest_archive_async_write_duration_seconds",
			Help:    "Duration of asynchronous raw archive writes.",
			Buckets: prometheus.DefBuckets,
		}),
		queuedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_archive_queued_total",
			Help: "Number of valid events queued for asynchronous raw archive writes.",
		}),
		writtenCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_archive_async_written_total",
			Help: "Number of queued events successfully written to the raw archive.",
		}),
		failedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_archive_async_failed_total",
			Help: "Number of queued raw archive writes that failed.",
		}),
		fullCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_ingest_archive_queue_full_total",
			Help: "Number of valid events rejected because the asynchronous raw archive queue was full.",
		}),
		depthGauge: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "pulsestream_ingest_archive_queue_depth",
			Help: "Current number of events waiting in the asynchronous raw archive queue.",
		}),
	}
	if registerer != nil {
		registerer.MustRegister(
			archiver.enqueueDuration,
			archiver.writeDuration,
			archiver.queuedCounter,
			archiver.writtenCounter,
			archiver.failedCounter,
			archiver.fullCounter,
			archiver.depthGauge,
		)
	}
	return archiver
}

func (a *AsyncArchiver) Start() {
	a.startOnce.Do(func() {
		for i := 0; i < a.workers; i++ {
			a.wg.Add(1)
			go a.runWorker()
		}
	})
}

func (a *AsyncArchiver) Archive(ctx context.Context, event events.TelemetryEvent, rawPayload []byte) error {
	if a == nil || a.next == nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	job := archiveJob{
		event:      event,
		rawPayload: append([]byte(nil), rawPayload...),
	}

	start := time.Now()
	a.mu.RLock()
	defer a.mu.RUnlock()
	if a.closed {
		return ErrArchiveClosed
	}

	timer := time.NewTimer(a.enqueueTimeout)
	defer timer.Stop()

	select {
	case a.queue <- job:
		a.observeEnqueueDuration(start)
		if a.queuedCounter != nil {
			a.queuedCounter.Inc()
		}
		if a.depthGauge != nil {
			a.depthGauge.Set(float64(len(a.queue)))
		}
		return nil
	case <-ctx.Done():
		a.observeEnqueueDuration(start)
		return ctx.Err()
	case <-timer.C:
		if a.fullCounter != nil {
			a.fullCounter.Inc()
		}
		a.observeEnqueueDuration(start)
		return ErrArchiveQueueFull
	}
}

func (a *AsyncArchiver) Close(ctx context.Context) error {
	a.closeOnce.Do(func() {
		a.mu.Lock()
		a.closed = true
		close(a.queue)
		a.mu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *AsyncArchiver) runWorker() {
	defer a.wg.Done()
	for job := range a.queue {
		if a.depthGauge != nil {
			a.depthGauge.Set(float64(len(a.queue)))
		}

		ctx, cancel := context.WithTimeout(context.Background(), a.writeTimeout)
		start := time.Now()
		err := a.next.Archive(ctx, job.event, job.rawPayload)
		cancel()
		if a.writeDuration != nil {
			a.writeDuration.Observe(time.Since(start).Seconds())
		}
		if err != nil {
			if a.failedCounter != nil {
				a.failedCounter.Inc()
			}
			if a.logger != nil {
				a.logger.Error("async_archive_write_failed", "error", err, "tenant_id", job.event.TenantID, "source_id", job.event.SourceID)
			}
			continue
		}
		if a.writtenCounter != nil {
			a.writtenCounter.Inc()
		}
	}
	if a.depthGauge != nil {
		a.depthGauge.Set(0)
	}
}

func (a *AsyncArchiver) observeEnqueueDuration(start time.Time) {
	if a.enqueueDuration != nil {
		a.enqueueDuration.Observe(time.Since(start).Seconds())
	}
}
