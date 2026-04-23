package archive

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"pulsestream/internal/events"
)

const dateLayout = "2006-01-02"

type Record struct {
	ArchivedAt time.Time             `json:"archived_at"`
	Event      events.TelemetryEvent `json:"event"`
	RawPayload json.RawMessage       `json:"raw_payload"`
}

type ReplayFilter struct {
	StartDate time.Time
	EndDate   time.Time
	TenantID  string
	Limit     int
}

type ReplayResult struct {
	StartDate   time.Time `json:"start_date"`
	EndDate     time.Time `json:"end_date"`
	TenantID    string    `json:"tenant_id,omitempty"`
	FilesRead   int       `json:"files_read"`
	Scanned     int64     `json:"scanned"`
	Skipped     int64     `json:"skipped"`
	Replayed    int64     `json:"replayed"`
	CompletedAt time.Time `json:"completed_at"`
}

type FileArchive struct {
	rootDir string

	mu      sync.Mutex
	writers map[string]*os.File
}

func NewFileArchive(rootDir string) *FileArchive {
	return &FileArchive{
		rootDir: filepath.Clean(rootDir),
		writers: map[string]*os.File{},
	}
}

func (a *FileArchive) Archive(ctx context.Context, event events.TelemetryEvent, rawPayload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if len(rawPayload) == 0 || !json.Valid(rawPayload) {
		encoded, err := events.EncodeTelemetryEvent(event)
		if err != nil {
			return fmt.Errorf("encode event for archive: %w", err)
		}
		rawPayload = encoded
	}

	record := Record{
		ArchivedAt: time.Now().UTC(),
		Event:      event,
		RawPayload: append(json.RawMessage(nil), rawPayload...),
	}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal archive record: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	writerTimestamp := event.Timestamp.UTC()
	if writerTimestamp.IsZero() {
		writerTimestamp = record.ArchivedAt
	}
	writer, err := a.writerForLocked(writerTimestamp, event.TenantID)
	if err != nil {
		return err
	}
	if _, err := writer.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("append archive record: %w", err)
	}

	return nil
}

func (a *FileArchive) Replay(
	ctx context.Context,
	filter ReplayFilter,
	publish func(context.Context, events.TelemetryEvent) error,
) (ReplayResult, error) {
	if filter.StartDate.IsZero() {
		return ReplayResult{}, errors.New("start_date is required")
	}
	endDate := filter.EndDate
	if endDate.IsZero() {
		endDate = filter.StartDate
	}
	startDate := normalizeDate(filter.StartDate)
	endDate = normalizeDate(endDate)
	if endDate.Before(startDate) {
		return ReplayResult{}, errors.New("end_date must be on or after start_date")
	}

	result := ReplayResult{
		StartDate: startDate,
		EndDate:   endDate,
		TenantID:  strings.TrimSpace(filter.TenantID),
	}

	for day := startDate; !day.After(endDate); day = day.AddDate(0, 0, 1) {
		if err := ctx.Err(); err != nil {
			return result, err
		}

		paths, err := a.pathsForReplayDay(day, result.TenantID)
		if err != nil {
			return result, err
		}
		for _, path := range paths {
			file, err := os.Open(path)
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				return result, fmt.Errorf("open archive file %s: %w", path, err)
			}

			result.FilesRead++
			scanner := bufio.NewScanner(file)
			scanner.Buffer(make([]byte, 0, 64*1024), 2<<20)

			for scanner.Scan() {
				if err := ctx.Err(); err != nil {
					_ = file.Close()
					return result, err
				}

				var record Record
				if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
					_ = file.Close()
					return result, fmt.Errorf("decode archive record from %s: %w", path, err)
				}

				result.Scanned++
				if result.TenantID != "" && record.Event.TenantID != result.TenantID {
					result.Skipped++
					continue
				}

				if filter.Limit > 0 && result.Replayed >= int64(filter.Limit) {
					_ = file.Close()
					result.CompletedAt = time.Now().UTC()
					return result, nil
				}

				if err := publish(ctx, record.Event); err != nil {
					_ = file.Close()
					return result, fmt.Errorf("publish replayed event %s: %w", record.Event.EventID, err)
				}
				result.Replayed++
			}

			if err := scanner.Err(); err != nil {
				_ = file.Close()
				return result, fmt.Errorf("scan archive file %s: %w", path, err)
			}
			_ = file.Close()
		}
	}

	result.CompletedAt = time.Now().UTC()
	return result, nil
}

func (a *FileArchive) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var closeErr error
	for path, file := range a.writers {
		if err := file.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close archive file %s: %w", path, err)
		}
		delete(a.writers, path)
	}
	return closeErr
}

func ParseDate(value string) (time.Time, error) {
	parsed, err := time.Parse(dateLayout, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse date %q: %w", value, err)
	}
	return normalizeDate(parsed), nil
}

func (a *FileArchive) writerForLocked(timestamp time.Time, tenantID string) (*os.File, error) {
	if a.writers == nil {
		a.writers = map[string]*os.File{}
	}
	path := a.pathForEvent(timestamp, tenantID)
	if writer := a.writers[path]; writer != nil {
		return writer, nil
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("create archive directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open archive file %s: %w", path, err)
	}

	a.writers[path] = file
	return file, nil
}

func (a *FileArchive) pathForEvent(timestamp time.Time, tenantID string) string {
	utc := timestamp.UTC()
	if utc.IsZero() {
		utc = time.Now().UTC()
	}
	return a.pathForTenantHour(utc, tenantID, utc.Hour())
}

func (a *FileArchive) pathForDate(timestamp time.Time) string {
	utc := normalizeDate(timestamp)
	return filepath.Join(
		a.rootDir,
		utc.Format("2006"),
		utc.Format("01"),
		utc.Format("02"),
		"events.ndjson",
	)
}

func (a *FileArchive) pathForTenantHour(day time.Time, tenantID string, hour int) string {
	utc := normalizeDate(day)
	return filepath.Join(
		a.rootDir,
		utc.Format("2006"),
		utc.Format("01"),
		utc.Format("02"),
		archivePathSegment(tenantID),
		fmt.Sprintf("%02d", hour),
		"events.ndjson",
	)
}

func (a *FileArchive) pathsForReplayDay(day time.Time, tenantID string) ([]string, error) {
	seen := map[string]struct{}{}
	paths := []string{}
	add := func(path string) {
		if _, ok := seen[path]; ok {
			return
		}
		seen[path] = struct{}{}
		paths = append(paths, path)
	}

	if strings.TrimSpace(tenantID) != "" {
		for hour := 0; hour < 24; hour++ {
			add(a.pathForTenantHour(day, tenantID, hour))
		}
		add(a.pathForDate(day))
		return paths, nil
	}

	dayDir := filepath.Dir(a.pathForDate(day))
	if err := filepath.WalkDir(dayDir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Name() == "events.ndjson" {
			add(path)
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("walk archive directory %s: %w", dayDir, err)
	}
	add(a.pathForDate(day))
	return paths, nil
}

func archivePathSegment(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "_unknown"
	}
	value = strings.ReplaceAll(value, `/`, `_`)
	value = strings.ReplaceAll(value, `\`, `_`)
	return value
}

func normalizeDate(value time.Time) time.Time {
	utc := value.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}
