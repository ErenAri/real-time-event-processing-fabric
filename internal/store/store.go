package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"pulsestream/internal/events"
)

type Store struct {
	pool *pgxpool.Pool
}

const schemaLockID int64 = 2026041001
const serviceStateStaleAfter = 15 * time.Second

func New(ctx context.Context, connectionString string) (*Store, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	config.MaxConns = 10
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	store := &Store{pool: pool}
	if err := store.EnsureSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection for schema init: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, schemaLockID); err != nil {
		return fmt.Errorf("acquire schema lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, schemaLockID)
	}()

	schema := `
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS tenant_metrics (
    bucket_start TIMESTAMPTZ NOT NULL,
    tenant_id TEXT NOT NULL,
    events_count BIGINT NOT NULL DEFAULT 0,
    ok_count BIGINT NOT NULL DEFAULT 0,
    warn_count BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (bucket_start, tenant_id)
);

CREATE TABLE IF NOT EXISTS source_metrics (
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    events_count BIGINT NOT NULL DEFAULT 0,
    last_event_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, source_id)
);

CREATE TABLE IF NOT EXISTS rejection_events (
    id BIGSERIAL PRIMARY KEY,
    reason TEXT NOT NULL,
    tenant_id TEXT,
    source_id TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS service_state (
    service_name TEXT NOT NULL,
    instance_id TEXT NOT NULL DEFAULT 'default',
    payload JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE service_state
    ADD COLUMN IF NOT EXISTS instance_id TEXT;

UPDATE service_state
SET instance_id = 'default'
WHERE instance_id IS NULL;

ALTER TABLE service_state
    ALTER COLUMN instance_id SET DEFAULT 'default';

ALTER TABLE service_state
    ALTER COLUMN instance_id SET NOT NULL;

ALTER TABLE service_state
    DROP CONSTRAINT IF EXISTS service_state_pkey;

CREATE INDEX IF NOT EXISTS idx_tenant_metrics_tenant_bucket
    ON tenant_metrics (tenant_id, bucket_start DESC);

CREATE INDEX IF NOT EXISTS idx_source_metrics_events
    ON source_metrics (events_count DESC);

CREATE INDEX IF NOT EXISTS idx_rejection_events_created_at
    ON rejection_events (created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_service_state_service_instance
    ON service_state (service_name, instance_id);`

	if _, err := conn.Exec(ctx, schema); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	return nil
}

func (s *Store) RecordRejection(ctx context.Context, record RejectionRecord) error {
	payload := []byte(`{}`)
	if len(record.Payload) > 0 {
		payload = record.Payload
	}

	_, err := s.pool.Exec(
		ctx,
		`INSERT INTO rejection_events (reason, tenant_id, source_id, payload)
		 VALUES ($1, NULLIF($2, ''), NULLIF($3, ''), $4::jsonb)`,
		record.Reason,
		record.TenantID,
		record.SourceID,
		string(payload),
	)
	if err != nil {
		return fmt.Errorf("record rejection: %w", err)
	}
	return nil
}

func (s *Store) UpdateServiceState(ctx context.Context, serviceName string, instanceID string, snapshot any) error {
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("marshal service state: %w", err)
	}

	_, err = s.pool.Exec(
		ctx,
		`INSERT INTO service_state (service_name, instance_id, payload, updated_at)
		 VALUES ($1, $2, $3::jsonb, NOW())
		 ON CONFLICT (service_name, instance_id)
		 DO UPDATE SET payload = EXCLUDED.payload, updated_at = EXCLUDED.updated_at`,
		serviceName,
		instanceID,
		string(payload),
	)
	if err != nil {
		return fmt.Errorf("update service state: %w", err)
	}
	return nil
}

func (s *Store) RecordProcessedEvent(ctx context.Context, event events.TelemetryEvent) (bool, error) {
	bucket := event.Timestamp.UTC().Truncate(10 * time.Second)
	okCount, warnCount, errorCount := statusCounts(event.Status)
	var recorded bool
	err := s.pool.QueryRow(
		ctx,
		`WITH claimed AS (
		     INSERT INTO processed_events (event_id, tenant_id, processed_at)
		     VALUES ($1, $2, NOW())
		     ON CONFLICT (event_id) DO NOTHING
		     RETURNING 1
		 ),
		 tenant_upsert AS (
		     INSERT INTO tenant_metrics (
		         bucket_start, tenant_id, events_count, ok_count, warn_count, error_count, value_sum, last_event_at
		     )
		     SELECT $3, $2, 1, $4, $5, $6, $7, $8
		     FROM claimed
		     ON CONFLICT (bucket_start, tenant_id)
		     DO UPDATE SET
		         events_count = tenant_metrics.events_count + 1,
		         ok_count = tenant_metrics.ok_count + EXCLUDED.ok_count,
		         warn_count = tenant_metrics.warn_count + EXCLUDED.warn_count,
		         error_count = tenant_metrics.error_count + EXCLUDED.error_count,
		         value_sum = tenant_metrics.value_sum + EXCLUDED.value_sum,
		         last_event_at = GREATEST(tenant_metrics.last_event_at, EXCLUDED.last_event_at)
		     RETURNING 1
		 ),
		 source_upsert AS (
		     INSERT INTO source_metrics (tenant_id, source_id, events_count, last_event_at)
		     SELECT $2, $9, 1, $8
		     FROM claimed
		     ON CONFLICT (tenant_id, source_id)
		     DO UPDATE SET
		         events_count = source_metrics.events_count + 1,
		         last_event_at = GREATEST(source_metrics.last_event_at, EXCLUDED.last_event_at)
		     RETURNING 1
		 )
		 SELECT EXISTS(SELECT 1 FROM claimed)`,
		event.EventID,
		event.TenantID,
		bucket,
		okCount,
		warnCount,
		errorCount,
		event.Value,
		event.Timestamp.UTC(),
		event.SourceID,
	).Scan(&recorded)
	if err != nil {
		return false, fmt.Errorf("record processed event: %w", err)
	}
	return recorded, nil
}

func (s *Store) GetOverview(ctx context.Context) (Overview, error) {
	var processedLastMinute int64
	var errorLastMinute int64
	if err := s.pool.QueryRow(
		ctx,
		`SELECT
		     COALESCE(SUM(events_count), 0),
		     COALESCE(SUM(error_count), 0)
		 FROM tenant_metrics
		 WHERE bucket_start >= NOW() - INTERVAL '1 minute'`,
	).Scan(&processedLastMinute, &errorLastMinute); err != nil {
		return Overview{}, fmt.Errorf("query minute aggregates: %w", err)
	}

	var processedTotal int64
	if err := s.pool.QueryRow(
		ctx,
		`SELECT COALESCE(SUM(events_count), 0) FROM source_metrics`,
	).Scan(&processedTotal); err != nil {
		return Overview{}, fmt.Errorf("query processed total: %w", err)
	}

	rejections, err := s.RecentRejections(ctx, 10)
	if err != nil {
		return Overview{}, err
	}

	states, err := s.getStates(ctx)
	if err != nil {
		return Overview{}, err
	}

	overview := Overview{
		GeneratedAt:      time.Now().UTC(),
		ProcessedTotal:   processedTotal,
		EventsPerSecond:  float64(processedLastMinute) / 60.0,
		RecentRejections: rejections,
	}
	if processedLastMinute > 0 {
		overview.ErrorRate = float64(errorLastMinute) / float64(processedLastMinute)
	}

	if ingest := aggregateIngestStates(states["ingest-service"]); !ingest.LastSeenAt.IsZero() {
		overview.AcceptedTotal = ingest.AcceptedTotal
		overview.RejectedTotal = ingest.RejectedTotal
		overview.IngestLastSeenAt = &ingest.LastSeenAt
	}
	if processor := aggregateProcessorStates(states["stream-processor"]); processor.LastSeenAt != nil {
		overview.DuplicateTotal = processor.DuplicateTotal
		overview.ConsumerLag = processor.ConsumerLag
		overview.ProcessorInstances = processor.InstanceCount
		overview.ActivePartitions = processor.ActivePartitions
		overview.ProcessorInFlight = processor.InFlightMessages
		overview.ProcessingP50MS = processor.ProcessingP50MS
		overview.ProcessingP95MS = processor.ProcessingP95MS
		overview.ProcessingP99MS = processor.ProcessingP99MS
		overview.ProcessorLastSeenAt = processor.LastSeenAt
	}

	return overview, nil
}

func (s *Store) GetTenantSeries(ctx context.Context, tenantID string, window time.Duration) ([]TenantBucket, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT bucket_start, events_count, ok_count, warn_count, error_count,
		        CASE WHEN events_count = 0 THEN 0 ELSE value_sum / events_count END AS average_value
		 FROM tenant_metrics
		 WHERE tenant_id = $1 AND bucket_start >= NOW() - $2::interval
		 ORDER BY bucket_start ASC`,
		tenantID,
		formatInterval(window),
	)
	if err != nil {
		return nil, fmt.Errorf("query tenant series: %w", err)
	}
	defer rows.Close()

	var buckets []TenantBucket
	for rows.Next() {
		var bucket TenantBucket
		if err := rows.Scan(
			&bucket.BucketStart,
			&bucket.EventsCount,
			&bucket.OKCount,
			&bucket.WarnCount,
			&bucket.ErrorCount,
			&bucket.Average,
		); err != nil {
			return nil, fmt.Errorf("scan tenant series: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (s *Store) GetTopSources(ctx context.Context, tenantID string, limit int) ([]SourceMetric, error) {
	query := `SELECT tenant_id, source_id, events_count, last_event_at
	          FROM source_metrics`
	args := []any{}
	if tenantID != "" {
		query += ` WHERE tenant_id = $1`
		args = append(args, tenantID)
	}
	query += ` ORDER BY events_count DESC, last_event_at DESC`
	if len(args) == 0 {
		query += ` LIMIT $1`
		args = append(args, limit)
	} else {
		query += ` LIMIT $2`
		args = append(args, limit)
	}

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query top sources: %w", err)
	}
	defer rows.Close()

	var sources []SourceMetric
	for rows.Next() {
		var source SourceMetric
		if err := rows.Scan(&source.TenantID, &source.SourceID, &source.Events, &source.LastEvent); err != nil {
			return nil, fmt.Errorf("scan top source: %w", err)
		}
		sources = append(sources, source)
	}
	return sources, rows.Err()
}

func (s *Store) RecentRejections(ctx context.Context, limit int) ([]RecentRejection, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT id, reason, COALESCE(tenant_id, ''), COALESCE(source_id, ''), created_at
		 FROM rejection_events
		 ORDER BY created_at DESC
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("query rejections: %w", err)
	}
	defer rows.Close()

	var rejections []RecentRejection
	for rows.Next() {
		var rejection RecentRejection
		if err := rows.Scan(
			&rejection.ID,
			&rejection.Reason,
			&rejection.TenantID,
			&rejection.SourceID,
			&rejection.CreatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan rejection: %w", err)
		}
		rejections = append(rejections, rejection)
	}
	return rejections, rows.Err()
}

type stateEnvelope struct {
	ServiceName string
	InstanceID  string
	IngestState *IngestState
	Processor   *ProcessorState
	QueryState  *QueryState
	UpdatedAt   time.Time
}

func (s *Store) getStates(ctx context.Context) (map[string][]stateEnvelope, error) {
	rows, err := s.pool.Query(
		ctx,
		`SELECT service_name, instance_id, payload, updated_at
		 FROM service_state
		 WHERE updated_at >= NOW() - $1::interval`,
		formatInterval(serviceStateStaleAfter),
	)
	if err != nil {
		return nil, fmt.Errorf("query service states: %w", err)
	}
	defer rows.Close()

	results := make(map[string][]stateEnvelope)
	for rows.Next() {
		var serviceName string
		var instanceID string
		var payload []byte
		var updatedAt time.Time
		if err := rows.Scan(&serviceName, &instanceID, &payload, &updatedAt); err != nil {
			return nil, fmt.Errorf("scan service state: %w", err)
		}

		switch serviceName {
		case "ingest-service":
			var state IngestState
			if err := json.Unmarshal(payload, &state); err != nil {
				return nil, fmt.Errorf("unmarshal ingest state: %w", err)
			}
			state.LastSeenAt = updatedAt.UTC()
			results[serviceName] = append(results[serviceName], stateEnvelope{
				ServiceName: serviceName,
				InstanceID:  instanceID,
				IngestState: &state,
				UpdatedAt:   updatedAt.UTC(),
			})
		case "stream-processor":
			var state ProcessorState
			if err := json.Unmarshal(payload, &state); err != nil {
				return nil, fmt.Errorf("unmarshal processor state: %w", err)
			}
			state.LastSeenAt = updatedAt.UTC()
			results[serviceName] = append(results[serviceName], stateEnvelope{
				ServiceName: serviceName,
				InstanceID:  instanceID,
				Processor:   &state,
				UpdatedAt:   updatedAt.UTC(),
			})
		case "query-service":
			var state QueryState
			if err := json.Unmarshal(payload, &state); err != nil {
				return nil, fmt.Errorf("unmarshal query state: %w", err)
			}
			state.LastSeenAt = updatedAt.UTC()
			results[serviceName] = append(results[serviceName], stateEnvelope{
				ServiceName: serviceName,
				InstanceID:  instanceID,
				QueryState:  &state,
				UpdatedAt:   updatedAt.UTC(),
			})
		}
	}
	return results, rows.Err()
}

func statusCounts(status events.Status) (int64, int64, int64) {
	switch status {
	case events.StatusOK:
		return 1, 0, 0
	case events.StatusWarn:
		return 0, 1, 0
	default:
		return 0, 0, 1
	}
}

func formatInterval(value time.Duration) string {
	return fmt.Sprintf("%f seconds", value.Seconds())
}
