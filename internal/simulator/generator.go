package simulator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/events"
)

type Config struct {
	Endpoint         string
	RatePerSecond    int
	TenantCount      int
	SourcesPerTenant int
	MaxInFlight      int
	DuplicateEvery   int64
	MalformedEvery   int64
	BurstEvery       time.Duration
	BurstSize        int
	Seed             int64
}

type Generator struct {
	config Config
	client *http.Client
	logger *slog.Logger
	rng    *rand.Rand

	startedAt time.Time
	sequence  atomic.Int64
	sentTotal atomic.Int64
	failTotal atomic.Int64

	mu        sync.Mutex
	lastValid events.TelemetryEvent

	sentCounter   prometheus.Counter
	failedCounter prometheus.Counter
}

func NewGenerator(config Config, logger *slog.Logger, registry *prometheus.Registry) *Generator {
	maxInFlight := max(1, config.MaxInFlight)
	transport := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		DialContext:         (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:   true,
		MaxIdleConns:        maxInFlight * 2,
		MaxIdleConnsPerHost: maxInFlight * 2,
		MaxConnsPerHost:     maxInFlight * 2,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}

	generator := &Generator{
		config:    config,
		client:    &http.Client{Timeout: 10 * time.Second, Transport: transport},
		logger:    logger,
		rng:       rand.New(rand.NewSource(config.Seed)),
		startedAt: time.Now().UTC(),
		sentCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_simulator_sent_total",
			Help: "Number of events the simulator attempted to send.",
		}),
		failedCounter: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "pulsestream_simulator_failed_total",
			Help: "Number of simulator HTTP send failures.",
		}),
	}

	registry.MustRegister(generator.sentCounter, generator.failedCounter)
	return generator
}

func (g *Generator) Run(ctx context.Context) error {
	baseBatch := max(1, g.config.RatePerSecond/10)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	workerCount := max(1, g.config.MaxInFlight)
	workCh := make(chan int64, workerCount*4)
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case index, ok := <-workCh:
					if !ok {
						return
					}
					if err := g.sendOne(ctx, index); err != nil {
						g.failedCounter.Inc()
						g.failTotal.Add(1)
						g.logger.Error("simulator_send_failed", "error", err)
					}
				}
			}
		}()
	}
	defer func() {
		close(workCh)
		workers.Wait()
	}()

	var burstTicker <-chan time.Time
	if g.config.BurstEvery > 0 && g.config.BurstSize > 0 {
		t := time.NewTicker(g.config.BurstEvery)
		defer t.Stop()
		burstTicker = t.C
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := g.dispatchBatch(ctx, workCh, baseBatch); err != nil {
				return err
			}
		case <-burstTicker:
			if err := g.dispatchBatch(ctx, workCh, g.config.BurstSize); err != nil {
				return err
			}
		}
	}
}

func (g *Generator) dispatchBatch(ctx context.Context, workCh chan<- int64, size int) error {
	for i := 0; i < size; i++ {
		index := g.sequence.Add(1)
		select {
		case <-ctx.Done():
			return nil
		case workCh <- index:
		}
	}
	return nil
}

func (g *Generator) sendOne(ctx context.Context, index int64) error {
	event := g.nextEvent(index)

	var payload []byte
	var err error
	if g.config.MalformedEvery > 0 && index%g.config.MalformedEvery == 0 {
		payload = []byte(`{"malformed": true`)
	} else if g.config.DuplicateEvery > 0 && index%g.config.DuplicateEvery == 0 {
		g.mu.Lock()
		payload, err = json.Marshal(g.lastValid)
		g.mu.Unlock()
	} else {
		payload, err = json.Marshal(event)
		g.mu.Lock()
		g.lastValid = event
		g.mu.Unlock()
	}
	if err != nil {
		return fmt.Errorf("marshal simulated event: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, g.config.Endpoint, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := g.client.Do(request)
	if err != nil {
		return fmt.Errorf("post event: %w", err)
	}
	defer response.Body.Close()

	g.sentCounter.Inc()
	g.sentTotal.Add(1)
	if response.StatusCode >= http.StatusBadRequest {
		return fmt.Errorf("ingest returned %s", response.Status)
	}
	return nil
}

func (g *Generator) nextEvent(sequence int64) events.TelemetryEvent {
	tenant := fmt.Sprintf("tenant_%02d", 1+g.rng.Intn(max(1, g.config.TenantCount)))
	source := fmt.Sprintf("sensor_%03d", 1+g.rng.Intn(max(1, g.config.SourcesPerTenant)))
	statuses := []events.Status{events.StatusOK, events.StatusOK, events.StatusWarn, events.StatusError}
	status := statuses[g.rng.Intn(len(statuses))]

	return events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       fmt.Sprintf("%s-%s-%d", tenant, source, sequence),
		TenantID:      tenant,
		SourceID:      source,
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         50 + g.rng.Float64()*50,
		Status:        status,
		Region:        []string{"us-east", "eu-west", "ap-south"}[g.rng.Intn(3)],
		Sequence:      sequence,
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
