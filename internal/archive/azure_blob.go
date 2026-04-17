package archive

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/google/uuid"

	"pulsestream/internal/events"
)

const (
	defaultAzureBlobFlushInterval = 5 * time.Second
	defaultAzureBlobFlushBytes    = 256 * 1024
)

type blobStore interface {
	EnsureContainer(ctx context.Context) error
	Upload(ctx context.Context, blobName string, data []byte) error
	List(ctx context.Context, prefix string) ([]string, error)
	Download(ctx context.Context, blobName string) (io.ReadCloser, error)
}

type AzureBlobArchive struct {
	store         blobStore
	prefix        string
	flushInterval time.Duration
	flushBytes    int

	mu          sync.Mutex
	currentKey  string
	buffer      bytes.Buffer
	bufferSince time.Time

	now       func() time.Time
	stopCh    chan struct{}
	doneCh    chan struct{}
	closeOnce sync.Once
	closeErr  error
}

type liveBlobStore struct {
	client    *azblob.Client
	container string
}

func NewAzureBlobArchive(ctx context.Context, config AzureBlobConfig) (*AzureBlobArchive, error) {
	store, err := newLiveBlobStore(ctx, config)
	if err != nil {
		return nil, err
	}
	return newAzureBlobArchiveWithStore(ctx, store, config)
}

func newAzureBlobArchiveWithStore(ctx context.Context, store blobStore, config AzureBlobConfig) (*AzureBlobArchive, error) {
	if err := store.EnsureContainer(ctx); err != nil {
		return nil, err
	}

	flushInterval := config.FlushInterval
	if flushInterval <= 0 {
		flushInterval = defaultAzureBlobFlushInterval
	}
	flushBytes := config.FlushBytes
	if flushBytes <= 0 {
		flushBytes = defaultAzureBlobFlushBytes
	}

	archive := &AzureBlobArchive{
		store:         store,
		prefix:        strings.Trim(strings.TrimSpace(config.Prefix), "/"),
		flushInterval: flushInterval,
		flushBytes:    flushBytes,
		now:           func() time.Time { return time.Now().UTC() },
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}

	go archive.runPeriodicFlush()
	return archive, nil
}

func (a *AzureBlobArchive) Archive(ctx context.Context, event events.TelemetryEvent, rawPayload []byte) error {
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
		ArchivedAt: a.now(),
		Event:      event,
		RawPayload: append(json.RawMessage(nil), rawPayload...),
	}
	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal archive record: %w", err)
	}

	timestamp := event.Timestamp.UTC()
	if timestamp.IsZero() {
		timestamp = record.ArchivedAt
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.rotateBufferLocked(ctx, a.eventPrefix(timestamp, event.TenantID)); err != nil {
		return err
	}
	if _, err := a.buffer.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("buffer archive record: %w", err)
	}
	if a.buffer.Len() >= a.flushBytes {
		if err := a.flushLocked(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (a *AzureBlobArchive) Replay(
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

		names, err := a.blobsForReplayDay(ctx, day, result.TenantID)
		if err != nil {
			return result, err
		}
		sort.Strings(names)

		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return result, err
			}

			reader, err := a.store.Download(ctx, name)
			if err != nil {
				return result, fmt.Errorf("download archive blob %s: %w", name, err)
			}

			result.FilesRead++
			scanner := bufio.NewScanner(reader)
			scanner.Buffer(make([]byte, 0, 64*1024), 2<<20)

			for scanner.Scan() {
				if err := ctx.Err(); err != nil {
					_ = reader.Close()
					return result, err
				}

				var record Record
				if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
					_ = reader.Close()
					return result, fmt.Errorf("decode archive record from %s: %w", name, err)
				}

				result.Scanned++
				if result.TenantID != "" && record.Event.TenantID != result.TenantID {
					result.Skipped++
					continue
				}

				if filter.Limit > 0 && result.Replayed >= int64(filter.Limit) {
					_ = reader.Close()
					result.CompletedAt = a.now()
					return result, nil
				}

				if err := publish(ctx, record.Event); err != nil {
					_ = reader.Close()
					return result, fmt.Errorf("publish replayed event %s: %w", record.Event.EventID, err)
				}
				result.Replayed++
			}

			if err := scanner.Err(); err != nil {
				_ = reader.Close()
				return result, fmt.Errorf("scan archive blob %s: %w", name, err)
			}
			_ = reader.Close()
		}
	}

	result.CompletedAt = a.now()
	return result, nil
}

func (a *AzureBlobArchive) Close() error {
	a.closeOnce.Do(func() {
		close(a.stopCh)
		<-a.doneCh

		a.mu.Lock()
		defer a.mu.Unlock()

		a.closeErr = a.flushLocked(context.Background())
	})
	return a.closeErr
}

func (a *AzureBlobArchive) runPeriodicFlush() {
	ticker := time.NewTicker(a.flushInterval)
	defer ticker.Stop()
	defer close(a.doneCh)

	for {
		select {
		case <-a.stopCh:
			return
		case <-ticker.C:
			a.mu.Lock()
			if a.buffer.Len() > 0 && !a.bufferSince.IsZero() && a.now().Sub(a.bufferSince) >= a.flushInterval {
				_ = a.flushLocked(context.Background())
			}
			a.mu.Unlock()
		}
	}
}

func (a *AzureBlobArchive) rotateBufferLocked(ctx context.Context, key string) error {
	if a.currentKey == "" {
		a.currentKey = key
		a.bufferSince = a.now()
		return nil
	}
	if a.currentKey == key {
		return nil
	}
	if err := a.flushLocked(ctx); err != nil {
		return err
	}
	a.currentKey = key
	a.bufferSince = a.now()
	return nil
}

func (a *AzureBlobArchive) flushLocked(ctx context.Context) error {
	if a.buffer.Len() == 0 || a.currentKey == "" {
		return nil
	}

	payload := append([]byte(nil), a.buffer.Bytes()...)
	blobName := a.blobName(a.currentKey)
	if err := a.store.Upload(ctx, blobName, payload); err != nil {
		return fmt.Errorf("upload archive blob %s: %w", blobName, err)
	}

	a.buffer.Reset()
	a.currentKey = ""
	a.bufferSince = time.Time{}
	return nil
}

func (a *AzureBlobArchive) blobName(prefix string) string {
	return path.Join(
		prefix,
		fmt.Sprintf("events-%s-%s.ndjson", a.now().Format("20060102T150405.000000000Z"), uuid.NewString()),
	)
}

func (a *AzureBlobArchive) eventPrefix(timestamp time.Time, tenantID string) string {
	timestamp = timestamp.UTC()
	if timestamp.IsZero() {
		timestamp = a.now()
	}
	return path.Join(a.dayPrefix(timestamp), archivePathSegment(tenantID), fmt.Sprintf("%02d", timestamp.Hour()))
}

func (a *AzureBlobArchive) dayPrefix(day time.Time) string {
	day = normalizeDate(day)
	if a.prefix == "" {
		return path.Join(day.Format("2006"), day.Format("01"), day.Format("02"))
	}
	return path.Join(a.prefix, day.Format("2006"), day.Format("01"), day.Format("02"))
}

func (a *AzureBlobArchive) blobsForReplayDay(ctx context.Context, day time.Time, tenantID string) ([]string, error) {
	seen := map[string]struct{}{}
	names := []string{}
	addNames := func(values []string) {
		for _, name := range values {
			if _, ok := seen[name]; ok {
				continue
			}
			seen[name] = struct{}{}
			names = append(names, name)
		}
	}

	if strings.TrimSpace(tenantID) != "" {
		for hour := 0; hour < 24; hour++ {
			prefix := path.Join(a.dayPrefix(day), archivePathSegment(tenantID), fmt.Sprintf("%02d", hour)) + "/"
			values, err := a.store.List(ctx, prefix)
			if err != nil {
				return nil, fmt.Errorf("list archive blobs with prefix %s: %w", prefix, err)
			}
			addNames(values)
		}
	}

	dayPrefix := a.dayPrefix(day)
	values, err := a.store.List(ctx, dayPrefix+"/")
	if err != nil {
		return nil, fmt.Errorf("list archive blobs with prefix %s: %w", dayPrefix+"/", err)
	}
	if strings.TrimSpace(tenantID) != "" {
		legacy := values[:0]
		for _, name := range values {
			remainder := strings.TrimPrefix(name, dayPrefix+"/")
			if !strings.Contains(remainder, "/") {
				legacy = append(legacy, name)
			}
		}
		values = legacy
	}
	addNames(values)
	return names, nil
}

func newLiveBlobStore(ctx context.Context, config AzureBlobConfig) (*liveBlobStore, error) {
	container := strings.TrimSpace(config.Container)
	if container == "" {
		return nil, errors.New("azure blob archive container is required")
	}

	var (
		client *azblob.Client
		err    error
	)
	if strings.TrimSpace(config.ConnectionString) != "" {
		client, err = azblob.NewClientFromConnectionString(config.ConnectionString, nil)
		if err != nil {
			return nil, fmt.Errorf("create blob client from connection string: %w", err)
		}
	} else {
		accountURL := strings.TrimSpace(config.AccountURL)
		if accountURL == "" {
			return nil, errors.New("azure blob archive account URL is required when connection string is not set")
		}
		credential, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			return nil, fmt.Errorf("create default azure credential: %w", err)
		}
		client, err = azblob.NewClient(accountURL, credential, nil)
		if err != nil {
			return nil, fmt.Errorf("create blob client with default credential: %w", err)
		}
	}

	store := &liveBlobStore{
		client:    client,
		container: container,
	}
	return store, nil
}

func (s *liveBlobStore) EnsureContainer(ctx context.Context) error {
	_, err := s.client.CreateContainer(ctx, s.container, nil)
	if err != nil && !bloberror.HasCode(err, bloberror.ContainerAlreadyExists) {
		return fmt.Errorf("ensure blob container %s: %w", s.container, err)
	}
	return nil
}

func (s *liveBlobStore) Upload(ctx context.Context, blobName string, data []byte) error {
	_, err := s.client.UploadBuffer(ctx, s.container, blobName, data, nil)
	if err != nil {
		return fmt.Errorf("upload blob %s: %w", blobName, err)
	}
	return nil
}

func (s *liveBlobStore) List(ctx context.Context, prefix string) ([]string, error) {
	options := &azblob.ListBlobsFlatOptions{
		Prefix: &prefix,
	}
	pager := s.client.NewListBlobsFlatPager(s.container, options)

	var names []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("list blobs with prefix %s: %w", prefix, err)
		}
		if page.Segment == nil {
			continue
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			names = append(names, *item.Name)
		}
	}
	return names, nil
}

func (s *liveBlobStore) Download(ctx context.Context, blobName string) (io.ReadCloser, error) {
	response, err := s.client.DownloadStream(ctx, s.container, blobName, nil)
	if err != nil {
		return nil, fmt.Errorf("download blob %s: %w", blobName, err)
	}
	return response.NewRetryReader(ctx, &blob.RetryReaderOptions{}), nil
}
