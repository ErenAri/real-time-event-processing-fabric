package archive

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"pulsestream/internal/events"
)

func TestFileArchiveRoundTripReplay(t *testing.T) {
	rootDir := t.TempDir()
	archiver := NewFileArchive(rootDir)
	t.Cleanup(func() {
		_ = archiver.Close()
	})

	first := events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-1",
		TenantID:      "tenant_1",
		SourceID:      "sensor_1",
		EventType:     "telemetry",
		Timestamp:     time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
		Value:         10,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	}
	second := events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-2",
		TenantID:      "tenant_2",
		SourceID:      "sensor_2",
		EventType:     "telemetry",
		Timestamp:     time.Date(2026, 4, 10, 12, 1, 0, 0, time.UTC),
		Value:         11,
		Status:        events.StatusWarn,
		Region:        "us-east",
		Sequence:      2,
	}

	if err := archiver.Archive(context.Background(), first, []byte(`{"event_id":"evt-1"}`)); err != nil {
		t.Fatalf("archive first event: %v", err)
	}
	if err := archiver.Archive(context.Background(), second, []byte(`{"event_id":"evt-2"}`)); err != nil {
		t.Fatalf("archive second event: %v", err)
	}

	archivePath := filepath.Join(rootDir, "2026", "04", "10", "tenant_1", "12", "events.ndjson")
	if _, err := ParseDate("2026-04-10"); err != nil {
		t.Fatalf("parse date: %v", err)
	}
	if _, err := os.Stat(archivePath); err != nil {
		t.Fatalf("expected indexed archive path to exist: %v", err)
	}

	var published []events.TelemetryEvent
	result, err := archiver.Replay(context.Background(), ReplayFilter{
		StartDate: time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC),
		TenantID:  "tenant_1",
	}, func(_ context.Context, event events.TelemetryEvent) error {
		published = append(published, event)
		return nil
	})
	if err != nil {
		t.Fatalf("replay archive: %v", err)
	}

	if result.Replayed != 1 {
		t.Fatalf("expected one replayed event, got %d", result.Replayed)
	}
	if len(published) != 1 || published[0].EventID != "evt-1" {
		t.Fatalf("unexpected replayed events: %+v", published)
	}
}

func TestFileArchiveReplayReadsLegacyDateOnlyLayout(t *testing.T) {
	rootDir := t.TempDir()
	archiver := NewFileArchive(rootDir)

	event := events.TelemetryEvent{
		SchemaVersion: events.CurrentSchemaVersion,
		EventID:       "evt-legacy",
		TenantID:      "tenant_legacy",
		SourceID:      "sensor_1",
		EventType:     "telemetry",
		Timestamp:     time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
		Value:         10,
		Status:        events.StatusOK,
		Region:        "eu-west",
		Sequence:      1,
	}
	record, err := json.Marshal(Record{
		ArchivedAt: time.Date(2026, 4, 10, 12, 1, 0, 0, time.UTC),
		Event:      event,
		RawPayload: []byte(`{"event_id":"evt-legacy"}`),
	})
	if err != nil {
		t.Fatalf("marshal legacy record: %v", err)
	}

	legacyPath := filepath.Join(rootDir, "2026", "04", "10", "events.ndjson")
	if err := os.MkdirAll(filepath.Dir(legacyPath), 0o755); err != nil {
		t.Fatalf("create legacy archive directory: %v", err)
	}
	if err := os.WriteFile(legacyPath, append(record, '\n'), 0o644); err != nil {
		t.Fatalf("write legacy archive file: %v", err)
	}

	var published []events.TelemetryEvent
	result, err := archiver.Replay(context.Background(), ReplayFilter{
		StartDate: time.Date(2026, 4, 10, 0, 0, 0, 0, time.UTC),
		TenantID:  "tenant_legacy",
	}, func(_ context.Context, event events.TelemetryEvent) error {
		published = append(published, event)
		return nil
	})
	if err != nil {
		t.Fatalf("replay legacy archive: %v", err)
	}
	if result.FilesRead != 1 || result.Replayed != 1 {
		t.Fatalf("expected one legacy file and one replayed event, got files=%d replayed=%d", result.FilesRead, result.Replayed)
	}
	if len(published) != 1 || published[0].EventID != "evt-legacy" {
		t.Fatalf("unexpected legacy replay events: %+v", published)
	}
}
