package processor

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"pulsestream/internal/deadletter"
	"pulsestream/internal/events"
	"pulsestream/internal/telemetry"
)

type fakeReader struct {
	messages chan kafka.Message

	mu         sync.Mutex
	committed  []kafka.Message
	fetchErrs  []error
	commitErrs []error
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
	f.mu.Lock()
	if len(f.fetchErrs) > 0 {
		err := f.fetchErrs[0]
		f.fetchErrs = f.fetchErrs[1:]
		f.mu.Unlock()
		return kafka.Message{}, err
	}
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		return kafka.Message{}, ctx.Err()
	case message := <-f.messages:
		return message, nil
	}
}

func (f *fakeReader) CommitMessages(ctx context.Context, messages ...kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.commitErrs) > 0 {
		err := f.commitErrs[0]
		f.commitErrs = f.commitErrs[1:]
		return err
	}
	f.committed = append(f.committed, messages...)
	return nil
}

func (f *fakeReader) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{}
}

type fakeDeadLetterPublisher struct {
	mu      sync.Mutex
	records []deadletter.Record
	err     error
}

func (f *fakeDeadLetterPublisher) PublishDeadLetter(_ context.Context, record deadletter.Record) error {
	if f.err != nil {
		return f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records = append(f.records, record)
	return nil
}

type blockingStore struct {
	startedBlocked chan struct{}
	startedFast    chan struct{}
	releaseBlocked chan struct{}
}

func (s *blockingStore) RecordProcessedEvent(ctx context.Context, event events.TelemetryEvent) (bool, error) {
	switch event.SourceID {
	case "sensor_blocked":
		close(s.startedBlocked)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-s.releaseBlocked:
		}
	case "sensor_fast":
		close(s.startedFast)
	}

	return true, nil
}

type successfulStore struct{}

func (s *successfulStore) RecordProcessedEvent(context.Context, events.TelemetryEvent) (bool, error) {
	return true, nil
}

type countingStore struct {
	processed atomic.Int64
}

func (s *countingStore) RecordProcessedEvent(context.Context, events.TelemetryEvent) (bool, error) {
	s.processed.Add(1)
	return true, nil
}

func TestRunnerProcessesDifferentPartitionsInParallel(t *testing.T) {
	reader := &fakeReader{
		messages: make(chan kafka.Message, 2),
	}
	store := &blockingStore{
		startedBlocked: make(chan struct{}),
		startedFast:    make(chan struct{}),
		releaseBlocked: make(chan struct{}),
	}

	blockedPayload, err := events.EncodeTelemetryEvent(events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-blocked",
		TenantID:      "tenant_01",
		SourceID:      "sensor_blocked",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         10,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	})
	if err != nil {
		t.Fatalf("encode blocked event: %v", err)
	}
	fastPayload, err := events.EncodeTelemetryEvent(events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-fast",
		TenantID:      "tenant_01",
		SourceID:      "sensor_fast",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         20,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      2,
	})
	if err != nil {
		t.Fatalf("encode fast event: %v", err)
	}

	reader.messages <- kafka.Message{Partition: 0, Offset: 1, Value: blockedPayload}
	reader.messages <- kafka.Message{Partition: 1, Offset: 1, Value: fastPayload}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runner := NewRunner(reader, store, nil, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		PartitionQueueCapacity: 8,
	})

	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	select {
	case <-store.startedBlocked:
	case <-time.After(2 * time.Second):
		t.Fatal("blocked partition did not start")
	}

	select {
	case <-store.startedFast:
	case <-time.After(2 * time.Second):
		t.Fatal("fast partition did not run while blocked partition was waiting")
	}

	close(store.releaseBlocked)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}

	reader.mu.Lock()
	defer reader.mu.Unlock()
	if len(reader.committed) != 2 {
		t.Fatalf("expected 2 committed messages, got %d", len(reader.committed))
	}
}

func nilLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRunnerDeadLettersMalformedMessageAndCommitsIt(t *testing.T) {
	reader := &fakeReader{}
	dlq := &fakeDeadLetterPublisher{}
	runner := NewRunner(reader, nil, dlq, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		ConsumerGroup: "pulsestream-processor",
	})

	message := kafka.Message{
		Topic:     "pulsestream.events",
		Partition: 1,
		Offset:    42,
		Key:       []byte("tenant_01:sensor_001"),
		Value:     []byte(`{"schema_version":1`),
	}

	if err := runner.handleMessage(context.Background(), message); err != nil {
		t.Fatalf("handle malformed message: %v", err)
	}

	reader.mu.Lock()
	defer reader.mu.Unlock()
	if len(reader.committed) != 1 {
		t.Fatalf("expected malformed message to be committed after DLQ publish, got %d commits", len(reader.committed))
	}
	if len(dlq.records) != 1 {
		t.Fatalf("expected one dead-letter record, got %d", len(dlq.records))
	}
	if dlq.records[0].Reason != "decode_failed" {
		t.Fatalf("expected decode_failed reason, got %q", dlq.records[0].Reason)
	}
	if runner.deadLetterTotal.Load() != 1 {
		t.Fatalf("expected dead-letter total to be 1, got %d", runner.deadLetterTotal.Load())
	}
}

func TestRunnerDeadLettersValidationFailureAndCommitsIt(t *testing.T) {
	reader := &fakeReader{}
	dlq := &fakeDeadLetterPublisher{}
	runner := NewRunner(reader, nil, dlq, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		ConsumerGroup: "pulsestream-processor",
	})

	payload := []byte(`{"schema_version":1,"event_id":"evt-1","tenant_id":"tenant_01","source_id":"sensor_001","event_type":"telemetry","timestamp":"2026-04-11T13:00:00Z","value":5,"status":"ok","region":"eu-west","sequence":0}`)
	message := kafka.Message{
		Topic:     "pulsestream.events",
		Partition: 0,
		Offset:    7,
		Key:       []byte("tenant_01:sensor_001"),
		Value:     payload,
	}

	if err := runner.handleMessage(context.Background(), message); err != nil {
		t.Fatalf("handle invalid message: %v", err)
	}

	if len(dlq.records) != 1 {
		t.Fatalf("expected one dead-letter record, got %d", len(dlq.records))
	}
	if dlq.records[0].Reason != "validation_failed" {
		t.Fatalf("expected validation_failed reason, got %q", dlq.records[0].Reason)
	}
	if dlq.records[0].TenantID != "tenant_01" {
		t.Fatalf("expected tenant metadata on dead-letter record, got %q", dlq.records[0].TenantID)
	}
}

func TestRunnerReturnsErrorWhenDeadLetterPublishFails(t *testing.T) {
	reader := &fakeReader{}
	dlq := &fakeDeadLetterPublisher{err: context.DeadlineExceeded}
	runner := NewRunner(reader, nil, dlq, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		ConsumerGroup: "pulsestream-processor",
	})

	message := kafka.Message{
		Topic:     "pulsestream.events",
		Partition: 1,
		Offset:    42,
		Key:       []byte("tenant_01:sensor_001"),
		Value:     []byte(`{"schema_version":1`),
	}

	if err := runner.handleMessage(context.Background(), message); err == nil {
		t.Fatal("expected dead-letter publish failure to bubble up")
	}

	reader.mu.Lock()
	defer reader.mu.Unlock()
	if len(reader.committed) != 0 {
		t.Fatalf("expected zero commits when dead-letter publish fails, got %d", len(reader.committed))
	}
}

func TestRunnerCreatesProcessAndCommitSpansFromKafkaHeaders(t *testing.T) {
	previousProvider := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := tracesdk.NewTracerProvider(tracesdk.WithSpanProcessor(recorder))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)
	defer func() {
		otel.SetTracerProvider(previousProvider)
		otel.SetTextMapPropagator(previousPropagator)
		_ = provider.Shutdown(context.Background())
	}()

	reader := &fakeReader{}
	store := &successfulStore{}
	runner := NewRunner(reader, store, nil, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		Brokers:       []string{"kafka:9092"},
		ConsumerGroup: "pulsestream-processor",
	})

	payload, err := events.EncodeTelemetryEvent(events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-trace",
		TenantID:      "tenant_01",
		SourceID:      "sensor_01",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         17.4,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}

	ctx, parentSpan := otel.Tracer("test").Start(context.Background(), "send pulsestream.events")
	message := kafka.Message{
		Topic:     "pulsestream.events",
		Partition: 1,
		Offset:    42,
		Key:       []byte("tenant_01:sensor_01"),
		Value:     payload,
	}
	telemetry.InjectKafkaHeaders(ctx, &message.Headers)
	parentSpan.End()

	if err := runner.handleMessage(context.Background(), message); err != nil {
		t.Fatalf("handle message: %v", err)
	}

	var processSpanName string
	var processSpanID string
	foundProcess := false
	foundCommit := false
	for _, span := range recorder.Ended() {
		switch span.Name() {
		case "process pulsestream.events":
			foundProcess = true
			processSpanName = span.Name()
			processSpanID = span.SpanContext().SpanID().String()
			if span.Parent().SpanID() != parentSpan.SpanContext().SpanID() {
				t.Fatalf("process span parent mismatch: got %s want %s", span.Parent().SpanID(), parentSpan.SpanContext().SpanID())
			}
		case "commit pulsestream.events":
			foundCommit = true
			if processSpanID != "" && span.Parent().SpanID().String() != processSpanID {
				t.Fatalf("commit span parent mismatch: got %s want %s", span.Parent().SpanID(), processSpanID)
			}
		}
	}

	if !foundProcess {
		t.Fatalf("expected process span %q to be recorded", "process pulsestream.events")
	}
	if !foundCommit {
		t.Fatalf("expected commit span to be recorded under %q", processSpanName)
	}
}

func TestRunnerRetriesFetchErrorsUntilBrokerRecovers(t *testing.T) {
	reader := &fakeReader{
		messages:  make(chan kafka.Message, 1),
		fetchErrs: []error{errors.New("dial tcp: connect refused")},
	}
	store := &countingStore{}
	runner := NewRunner(reader, store, nil, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		RetryBackoff: 10 * time.Millisecond,
	})

	payload, err := events.EncodeTelemetryEvent(events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-fetch-retry",
		TenantID:      "tenant_01",
		SourceID:      "sensor_001",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         10,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}
	reader.messages <- kafka.Message{Partition: 0, Offset: 1, Value: payload}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- runner.Run(ctx)
	}()

	deadline := time.After(2 * time.Second)
	for store.processed.Load() == 0 {
		select {
		case <-deadline:
			cancel()
			t.Fatal("runner did not recover from fetch error")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner returned error after transient fetch failure: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not stop")
	}
}

func TestRunnerRetriesCommitErrorsUntilCommitSucceeds(t *testing.T) {
	reader := &fakeReader{
		messages:   make(chan kafka.Message, 1),
		commitErrs: []error{errors.New("commit failed"), errors.New("commit failed again")},
	}
	store := &successfulStore{}
	runner := NewRunner(reader, store, nil, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
		RetryBackoff: 10 * time.Millisecond,
	})

	payload, err := events.EncodeTelemetryEvent(events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-commit-retry",
		TenantID:      "tenant_01",
		SourceID:      "sensor_001",
		EventType:     "telemetry",
		Timestamp:     time.Now().UTC(),
		Value:         10,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	})
	if err != nil {
		t.Fatalf("encode event: %v", err)
	}

	if err := runner.handleMessage(context.Background(), kafka.Message{Topic: "pulsestream.events", Partition: 0, Offset: 1, Value: payload}); err != nil {
		t.Fatalf("handle message: %v", err)
	}

	reader.mu.Lock()
	defer reader.mu.Unlock()
	if len(reader.committed) != 1 {
		t.Fatalf("expected commit after retries, got %d committed messages", len(reader.committed))
	}
}
