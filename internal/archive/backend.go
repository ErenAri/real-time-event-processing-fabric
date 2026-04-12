package archive

import (
	"context"
	"fmt"
	"strings"
	"time"

	"pulsestream/internal/events"
)

const (
	BackendFilesystem = "filesystem"
	BackendAzureBlob  = "azure_blob"
)

type Archiver interface {
	Archive(ctx context.Context, event events.TelemetryEvent, rawPayload []byte) error
	Replay(ctx context.Context, filter ReplayFilter, publish func(context.Context, events.TelemetryEvent) error) (ReplayResult, error)
	Close() error
}

type Config struct {
	Backend   string
	FileRoot  string
	AzureBlob AzureBlobConfig
}

type AzureBlobConfig struct {
	AccountURL       string
	ConnectionString string
	Container        string
	Prefix           string
	FlushInterval    time.Duration
	FlushBytes       int
}

func New(ctx context.Context, config Config) (Archiver, error) {
	switch strings.TrimSpace(config.Backend) {
	case "", BackendFilesystem:
		return NewFileArchive(config.FileRoot), nil
	case BackendAzureBlob:
		return NewAzureBlobArchive(ctx, config.AzureBlob)
	default:
		return nil, fmt.Errorf("unsupported raw archive backend %q", config.Backend)
	}
}
