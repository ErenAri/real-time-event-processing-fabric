package api

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"pulsestream/internal/events"
	"pulsestream/internal/platform"
)

type recordingRawArchiver struct {
	mu      sync.Mutex
	events  []events.TelemetryEvent
	done    chan struct{}
	doneOne sync.Once
}

func newRecordingRawArchiver() *recordingRawArchiver {
	return &recordingRawArchiver{done: make(chan struct{})}
}

func (r *recordingRawArchiver) Archive(_ context.Context, event events.TelemetryEvent, _ []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.events = append(r.events, event)
	r.doneOne.Do(func() {
		close(r.done)
	})
	return nil
}

func TestAsyncArchiverQueuesAndWritesEvent(t *testing.T) {
	rawArchiver := newRecordingRawArchiver()
	asyncArchiver := NewAsyncArchiver(platform.NewLogger("test"), rawArchiver, prometheus.NewRegistry(), AsyncArchiverConfig{
		QueueCapacity:  4,
		Workers:        1,
		EnqueueTimeout: 50 * time.Millisecond,
		WriteTimeout:   time.Second,
	})
	asyncArchiver.Start()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = asyncArchiver.Close(ctx)
	}()

	event := events.TelemetryEvent{EventID: "evt-1", TenantID: "tenant_1", SourceID: "sensor_1"}
	if err := asyncArchiver.Archive(context.Background(), event, []byte(`{"event_id":"evt-1"}`)); err != nil {
		t.Fatalf("archive event: %v", err)
	}

	select {
	case <-rawArchiver.done:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for async archive write")
	}

	rawArchiver.mu.Lock()
	defer rawArchiver.mu.Unlock()
	if len(rawArchiver.events) != 1 {
		t.Fatalf("expected one archived event, got %d", len(rawArchiver.events))
	}
}

func TestAsyncArchiverReturnsQueueFullWhenBackpressured(t *testing.T) {
	rawArchiver := newRecordingRawArchiver()
	asyncArchiver := NewAsyncArchiver(platform.NewLogger("test"), rawArchiver, prometheus.NewRegistry(), AsyncArchiverConfig{
		QueueCapacity:  1,
		Workers:        1,
		EnqueueTimeout: time.Millisecond,
		WriteTimeout:   time.Second,
	})

	event := events.TelemetryEvent{EventID: "evt-1", TenantID: "tenant_1", SourceID: "sensor_1"}
	if err := asyncArchiver.Archive(context.Background(), event, []byte(`{"event_id":"evt-1"}`)); err != nil {
		t.Fatalf("archive first event: %v", err)
	}
	if err := asyncArchiver.Archive(context.Background(), event, []byte(`{"event_id":"evt-1"}`)); err != ErrArchiveQueueFull {
		t.Fatalf("expected queue full error, got %v", err)
	}
}
