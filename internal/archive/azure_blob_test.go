package archive

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"
	"time"

	"pulsestream/internal/events"
)

type fakeBlobStore struct {
	uploads     map[string][]byte
	ensureCount int
	uploadErr   error
	listErr     error
	downloadErr error
}

func (f *fakeBlobStore) EnsureContainer(_ context.Context) error {
	f.ensureCount++
	return nil
}

func (f *fakeBlobStore) Upload(_ context.Context, blobName string, data []byte) error {
	if f.uploadErr != nil {
		return f.uploadErr
	}
	if f.uploads == nil {
		f.uploads = map[string][]byte{}
	}
	f.uploads[blobName] = append([]byte(nil), data...)
	return nil
}

func (f *fakeBlobStore) List(_ context.Context, prefix string) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	var names []string
	for name := range f.uploads {
		if strings.HasPrefix(name, prefix) {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func (f *fakeBlobStore) Download(_ context.Context, blobName string) (io.ReadCloser, error) {
	if f.downloadErr != nil {
		return nil, f.downloadErr
	}
	payload, ok := f.uploads[blobName]
	if !ok {
		return nil, errors.New("blob not found")
	}
	return io.NopCloser(bytes.NewReader(payload)), nil
}

func TestAzureBlobArchiveRoundTripReplay(t *testing.T) {
	store := &fakeBlobStore{}
	archive, err := newAzureBlobArchiveWithStore(context.Background(), store, AzureBlobConfig{
		Container:     "archive",
		Prefix:        "raw",
		FlushBytes:    1024,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("create azure blob archive: %v", err)
	}
	t.Cleanup(func() {
		_ = archive.Close()
	})
	archive.now = func() time.Time {
		return time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	}

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

	if err := archive.Archive(context.Background(), first, []byte(`{"event_id":"evt-1"}`)); err != nil {
		t.Fatalf("archive first event: %v", err)
	}
	if err := archive.Archive(context.Background(), second, []byte(`{"event_id":"evt-2"}`)); err != nil {
		t.Fatalf("archive second event: %v", err)
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}

	if len(store.uploads) != 1 {
		t.Fatalf("expected one uploaded blob, got %d", len(store.uploads))
	}
	for blobName := range store.uploads {
		if !strings.HasPrefix(blobName, "raw/2026/04/10/events-") {
			t.Fatalf("unexpected blob name %q", blobName)
		}
	}

	var published []events.TelemetryEvent
	result, err := archive.Replay(context.Background(), ReplayFilter{
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

func TestAzureBlobArchiveFlushesOnDateChange(t *testing.T) {
	store := &fakeBlobStore{}
	archive, err := newAzureBlobArchiveWithStore(context.Background(), store, AzureBlobConfig{
		Container:     "archive",
		FlushBytes:    4096,
		FlushInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("create azure blob archive: %v", err)
	}
	t.Cleanup(func() {
		_ = archive.Close()
	})
	archive.now = func() time.Time {
		return time.Date(2026, 4, 12, 10, 0, 0, 0, time.UTC)
	}

	days := []time.Time{
		time.Date(2026, 4, 10, 12, 0, 0, 0, time.UTC),
		time.Date(2026, 4, 11, 12, 0, 0, 0, time.UTC),
	}
	for index, day := range days {
		err := archive.Archive(context.Background(), events.TelemetryEvent{
			SchemaVersion: events.CurrentSchemaVersion,
			EventID:       "evt-" + string(rune('1'+index)),
			TenantID:      "tenant_1",
			SourceID:      "sensor_1",
			EventType:     "telemetry",
			Timestamp:     day,
			Value:         10,
			Status:        events.StatusOK,
			Region:        "eu-west",
			Sequence:      int64(index + 1),
		}, nil)
		if err != nil {
			t.Fatalf("archive event for %s: %v", day.Format(dateLayout), err)
		}
	}
	if err := archive.Close(); err != nil {
		t.Fatalf("close archive: %v", err)
	}

	if len(store.uploads) != 2 {
		t.Fatalf("expected two uploaded blobs, got %d", len(store.uploads))
	}
}
