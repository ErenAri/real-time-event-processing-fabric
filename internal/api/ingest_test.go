package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	eventarchive "pulsestream/internal/archive"
	"pulsestream/internal/auth"
	"pulsestream/internal/events"
	"pulsestream/internal/platform"
	"pulsestream/internal/store"
)

type fakePublisher struct {
	published []events.TelemetryEvent
}

func (f *fakePublisher) PublishEvent(_ context.Context, event events.TelemetryEvent) error {
	f.published = append(f.published, event)
	return nil
}

func (f *fakePublisher) Close() error {
	return nil
}

type blockingPublisher struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *blockingPublisher) PublishEvent(ctx context.Context, event events.TelemetryEvent) error {
	f.once.Do(func() {
		close(f.started)
	})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-f.release:
		return nil
	}
}

func (f *blockingPublisher) Close() error {
	return nil
}

type fakeRejections struct {
	records []store.RejectionRecord
}

func (f *fakeRejections) RecordRejection(_ context.Context, record store.RejectionRecord) error {
	f.records = append(f.records, record)
	return nil
}

type fakeArchiver struct {
	events   []events.TelemetryEvent
	payloads [][]byte
}

func (f *fakeArchiver) Archive(_ context.Context, event events.TelemetryEvent, rawPayload []byte) error {
	f.events = append(f.events, event)
	f.payloads = append(f.payloads, append([]byte(nil), rawPayload...))
	return nil
}

type fakeReplayer struct {
	result eventarchive.ReplayResult
	events []events.TelemetryEvent
	filter eventarchive.ReplayFilter
}

func (f *fakeReplayer) Replay(
	ctx context.Context,
	filter eventarchive.ReplayFilter,
	publish func(context.Context, events.TelemetryEvent) error,
) (eventarchive.ReplayResult, error) {
	f.filter = filter
	for _, event := range f.events {
		if err := publish(ctx, event); err != nil {
			return eventarchive.ReplayResult{}, err
		}
	}
	if f.result.Replayed == 0 {
		f.result.Replayed = int64(len(f.events))
	}
	if f.result.StartDate.IsZero() {
		f.result.StartDate = filter.StartDate
	}
	if f.result.EndDate.IsZero() {
		f.result.EndDate = filter.EndDate
	}
	if f.result.CompletedAt.IsZero() {
		f.result.CompletedAt = time.Now().UTC()
	}
	return f.result, nil
}

func TestIngestHandlerAcceptsValidEvent(t *testing.T) {
	publisher := &fakePublisher{}
	recorder := &fakeRejections{}
	archiver := &fakeArchiver{}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, recorder, archiver, nil, nil, "", prometheus.NewRegistry())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{
		"schema_version":1,
		"event_id":"evt-1",
		"tenant_id":"tenant_1",
		"source_id":"sensor_1",
		"event_type":"telemetry",
		"timestamp":"2026-04-10T12:00:00Z",
		"value":91.2,
		"status":"ok",
		"region":"eu-west",
		"sequence":1
	}`))
	rec := httptest.NewRecorder()

	handler.RegisterRoutes(http.NewServeMux())
	handler.handleEvents(rec, request)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", rec.Code)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected one published event, got %d", len(publisher.published))
	}
	if len(archiver.events) != 1 {
		t.Fatalf("expected one archived event, got %d", len(archiver.events))
	}
	if len(recorder.records) != 0 {
		t.Fatalf("expected no rejections, got %d", len(recorder.records))
	}
}

func TestIngestHandlerRejectsMalformedPayload(t *testing.T) {
	publisher := &fakePublisher{}
	recorder := &fakeRejections{}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, recorder, nil, nil, nil, "", prometheus.NewRegistry())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{"event_id":"broken"`))
	rec := httptest.NewRecorder()

	handler.handleEvents(rec, request)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("expected zero published events, got %d", len(publisher.published))
	}
	if len(recorder.records) != 1 {
		t.Fatalf("expected one rejection, got %d", len(recorder.records))
	}
}

func TestIngestHandlerReplaysArchivedEventsForAuthorizedAdmin(t *testing.T) {
	publisher := &fakePublisher{}
	recorder := &fakeRejections{}
	replayer := &fakeReplayer{
		events: []events.TelemetryEvent{
			{
				SchemaVersion: events.CurrentSchemaVersion,
				EventID:       "evt-9",
				TenantID:      "tenant_1",
				SourceID:      "sensor_9",
				EventType:     "telemetry",
				Timestamp:     time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
				Value:         22.5,
				Status:        events.StatusOK,
				Region:        "eu-west",
				Sequence:      9,
			},
		},
	}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, recorder, nil, replayer, nil, "secret-admin", prometheus.NewRegistry())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/replay", strings.NewReader(`{
		"start_date":"2026-04-10",
		"tenant_id":"tenant_1",
		"limit":10
	}`))
	request.Header.Set("X-Admin-Token", "secret-admin")
	rec := httptest.NewRecorder()

	handler.handleReplay(rec, request)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected one replayed publish, got %d", len(publisher.published))
	}
	if replayer.filter.TenantID != "tenant_1" {
		t.Fatalf("expected tenant filter to be forwarded, got %q", replayer.filter.TenantID)
	}
}

func TestIngestHandlerRejectsTenantMismatchForTenantToken(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token, err := auth.SignHS256("secret-value", auth.NewClaims(
		auth.RoleTenantUser,
		"tenant_1",
		"user-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	publisher := &fakePublisher{}
	recorder := &fakeRejections{}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, recorder, nil, nil, verifier, "", prometheus.NewRegistry())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{
		"schema_version":1,
		"event_id":"evt-1",
		"tenant_id":"tenant_2",
		"source_id":"sensor_1",
		"event_type":"telemetry",
		"timestamp":"2026-04-10T12:00:00Z",
		"value":91.2,
		"status":"ok",
		"region":"eu-west",
		"sequence":1
	}`))
	request.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.handleEvents(rec, request)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", rec.Code)
	}
	if len(publisher.published) != 0 {
		t.Fatalf("expected zero published events, got %d", len(publisher.published))
	}
}

func TestIngestHandlerReplaysArchivedEventsForAdminJWT(t *testing.T) {
	verifier, err := auth.NewVerifier("secret-value", "pulsestream-local", "pulsestream-local")
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	token, err := auth.SignHS256("secret-value", auth.NewClaims(
		auth.RoleAdmin,
		"",
		"admin-1",
		"pulsestream-local",
		"pulsestream-local",
		time.Now().Add(time.Hour),
	))
	if err != nil {
		t.Fatalf("sign token: %v", err)
	}

	publisher := &fakePublisher{}
	replayer := &fakeReplayer{
		events: []events.TelemetryEvent{
			{
				SchemaVersion: events.CurrentSchemaVersion,
				EventID:       "evt-9",
				TenantID:      "tenant_1",
				SourceID:      "sensor_9",
				EventType:     "telemetry",
				Timestamp:     time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
				Value:         22.5,
				Status:        events.StatusOK,
				Region:        "eu-west",
				Sequence:      9,
			},
		},
	}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, nil, nil, replayer, verifier, "", prometheus.NewRegistry())

	request := httptest.NewRequest(http.MethodPost, "/api/v1/admin/replay", strings.NewReader(`{
		"start_date":"2026-04-10",
		"tenant_id":"tenant_1",
		"limit":10
	}`))
	request.Header.Set("Authorization", "Bearer "+token)
	rec := httptest.NewRecorder()

	handler.handleReplay(rec, request)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if len(publisher.published) != 1 {
		t.Fatalf("expected one replayed publish, got %d", len(publisher.published))
	}
}

func TestIngestHandlerRejectsWhenBackpressureLimitIsReached(t *testing.T) {
	publisher := &blockingPublisher{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	recorder := &fakeRejections{}
	handler := NewIngestHandler(platform.NewLogger("test"), publisher, recorder, nil, nil, nil, "", prometheus.NewRegistry())
	handler.SetMaxInFlight(1)

	firstRequest := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{
		"schema_version":1,
		"event_id":"evt-1",
		"tenant_id":"tenant_1",
		"source_id":"sensor_1",
		"event_type":"telemetry",
		"timestamp":"2026-04-10T12:00:00Z",
		"value":91.2,
		"status":"ok",
		"region":"eu-west",
		"sequence":1
	}`))
	firstRecorder := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		handler.handleEvents(firstRecorder, firstRequest)
		close(done)
	}()

	select {
	case <-publisher.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not occupy the in-flight slot")
	}

	secondRequest := httptest.NewRequest(http.MethodPost, "/api/v1/events", strings.NewReader(`{
		"schema_version":1,
		"event_id":"evt-2",
		"tenant_id":"tenant_1",
		"source_id":"sensor_2",
		"event_type":"telemetry",
		"timestamp":"2026-04-10T12:00:00Z",
		"value":91.2,
		"status":"ok",
		"region":"eu-west",
		"sequence":2
	}`))
	secondRecorder := httptest.NewRecorder()
	handler.handleEvents(secondRecorder, secondRequest)

	if secondRecorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", secondRecorder.Code)
	}
	if len(recorder.records) != 0 {
		t.Fatalf("expected backpressure to stay metric-only, got %d persisted records", len(recorder.records))
	}

	close(publisher.release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first request did not complete")
	}
	if firstRecorder.Code != http.StatusAccepted {
		t.Fatalf("expected first request to succeed, got %d", firstRecorder.Code)
	}
}
