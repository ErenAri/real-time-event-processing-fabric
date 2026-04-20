package store

import (
	"time"

	"pulsestream/internal/events"
)

type RejectionRecord struct {
	Reason   string
	TenantID string
	SourceID string
	Payload  []byte
}

type RecentRejection struct {
	ID        int64     `json:"id"`
	Reason    string    `json:"reason"`
	TenantID  string    `json:"tenant_id,omitempty"`
	SourceID  string    `json:"source_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

type TenantBucket struct {
	BucketStart time.Time `json:"bucket_start"`
	EventsCount int64     `json:"events_count"`
	OKCount     int64     `json:"ok_count"`
	WarnCount   int64     `json:"warn_count"`
	ErrorCount  int64     `json:"error_count"`
	Average     float64   `json:"average_value"`
}

type WindowBucket struct {
	WindowStart time.Time `json:"window_start"`
	WindowSize  string    `json:"window_size"`
	TenantID    string    `json:"tenant_id"`
	SourceID    string    `json:"source_id,omitempty"`
	EventsCount int64     `json:"events_count"`
	OKCount     int64     `json:"ok_count"`
	WarnCount   int64     `json:"warn_count"`
	ErrorCount  int64     `json:"error_count"`
	Average     float64   `json:"average_value"`
	MaxEventAt  time.Time `json:"max_event_at"`
	FreshnessMS float64   `json:"freshness_ms"`
}

type SourceMetric struct {
	TenantID  string    `json:"tenant_id"`
	SourceID  string    `json:"source_id"`
	Events    int64     `json:"events"`
	LastEvent time.Time `json:"last_event_at"`
}

type IngestState struct {
	AcceptedTotal int64     `json:"accepted_total"`
	RejectedTotal int64     `json:"rejected_total"`
	LastSeenAt    time.Time `json:"last_seen_at"`
	UptimeSeconds int64     `json:"uptime_seconds"`
}

type ProcessedEventInput struct {
	Event events.TelemetryEvent
	Late  bool
}

type ProcessorStageDurations struct {
	DedupClaimMS      float64 `json:"dedup_claim_ms"`
	TenantAggregateMS float64 `json:"tenant_aggregate_ms"`
	SourceAggregateMS float64 `json:"source_aggregate_ms"`
	WindowAggregateMS float64 `json:"window_aggregate_ms"`
	CommitMS          float64 `json:"commit_ms"`
}

type ProcessedEventBatchResult struct {
	RecordedCount  int64                   `json:"recorded_count"`
	DuplicateCount int64                   `json:"duplicate_count"`
	LateCount      int64                   `json:"late_count"`
	StageDurations ProcessorStageDurations `json:"stage_durations"`
}

type PartitionState struct {
	Partition        int       `json:"partition"`
	OwnerInstanceID  string    `json:"owner_instance_id,omitempty"`
	Lag              int64     `json:"lag"`
	ProcessedTotal   int64     `json:"processed_total"`
	DuplicateTotal   int64     `json:"duplicate_total"`
	LateEventTotal   int64     `json:"late_event_total"`
	InFlightMessages int64     `json:"inflight_messages"`
	LastOffset       int64     `json:"last_offset"`
	LastSeenAt       time.Time `json:"last_seen_at"`
}

type ProcessorState struct {
	ProcessedTotal   int64            `json:"processed_total"`
	DuplicateTotal   int64            `json:"duplicate_total"`
	LateEventTotal   int64            `json:"late_event_total"`
	DeadLetterTotal  int64            `json:"dead_letter_total"`
	ConsumerLag      int64            `json:"consumer_lag"`
	ActivePartitions int64            `json:"active_partitions"`
	InFlightMessages int64            `json:"inflight_messages"`
	LastSeenAt       time.Time        `json:"last_seen_at"`
	UptimeSeconds    int64            `json:"uptime_seconds"`
	ProcessingP50MS  float64          `json:"processing_p50_ms"`
	ProcessingP95MS  float64          `json:"processing_p95_ms"`
	ProcessingP99MS  float64          `json:"processing_p99_ms"`
	BatchSizeP50     float64          `json:"batch_size_p50"`
	BatchSizeP95     float64          `json:"batch_size_p95"`
	BatchSizeP99     float64          `json:"batch_size_p99"`
	BatchFlushP50MS  float64          `json:"batch_flush_p50_ms"`
	BatchFlushP95MS  float64          `json:"batch_flush_p95_ms"`
	BatchFlushP99MS  float64          `json:"batch_flush_p99_ms"`
	Partitions       []PartitionState `json:"partitions"`
}

type QueryState struct {
	RequestCount  int64     `json:"request_count"`
	LastSeenAt    time.Time `json:"last_seen_at"`
	UptimeSeconds int64     `json:"uptime_seconds"`
}

type Overview struct {
	GeneratedAt             time.Time         `json:"generated_at"`
	AcceptedTotal           int64             `json:"accepted_total"`
	RejectedTotal           int64             `json:"rejected_total"`
	ProcessedTotal          int64             `json:"processed_total"`
	StoredProcessedTotal    int64             `json:"stored_processed_total"`
	ProcessorProcessedTotal int64             `json:"processor_processed_total"`
	DuplicateTotal          int64             `json:"duplicate_total"`
	LateEventTotal          int64             `json:"late_event_total"`
	DeadLetterTotal         int64             `json:"dead_letter_total"`
	ConsumerLag             int64             `json:"consumer_lag"`
	ProcessorInstances      int               `json:"processor_instances"`
	ActivePartitions        int64             `json:"processor_active_partitions"`
	ProcessorInFlight       int64             `json:"processor_inflight_messages"`
	EventsPerSecond         float64           `json:"events_per_second_last_minute"`
	ErrorRate               float64           `json:"error_rate_last_minute"`
	ProcessingP50MS         float64           `json:"processing_p50_ms"`
	ProcessingP95MS         float64           `json:"processing_p95_ms"`
	ProcessingP99MS         float64           `json:"processing_p99_ms"`
	BatchSizeP95            float64           `json:"batch_size_p95"`
	BatchFlushP95MS         float64           `json:"batch_flush_p95_ms"`
	WindowSizes             []string          `json:"window_sizes"`
	AllowedLateness         string            `json:"allowed_lateness"`
	PartitionHealth         []PartitionState  `json:"partition_health"`
	IngestLastSeenAt        *time.Time        `json:"ingest_last_seen_at,omitempty"`
	ProcessorLastSeenAt     *time.Time        `json:"processor_last_seen_at,omitempty"`
	RecentRejections        []RecentRejection `json:"recent_rejections"`
}
