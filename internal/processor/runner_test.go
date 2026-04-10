package processor

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/segmentio/kafka-go"

	"pulsestream/internal/events"
)

type fakeReader struct {
	messages chan kafka.Message

	mu        sync.Mutex
	committed []kafka.Message
}

func (f *fakeReader) FetchMessage(ctx context.Context) (kafka.Message, error) {
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
	f.committed = append(f.committed, messages...)
	return nil
}

func (f *fakeReader) Stats() kafka.ReaderStats {
	return kafka.ReaderStats{}
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

	runner := NewRunner(reader, store, nilLogger(), prometheus.NewRegistry(), RunnerConfig{
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
