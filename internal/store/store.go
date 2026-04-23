package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"pulsestream/internal/auth"
	"pulsestream/internal/events"
)

type Store struct {
	pool        *pgxpool.Pool
	runtimeRole string
}

const schemaLockID int64 = 2026041001
const schemaVersion = "20260423_tenant_metric_shards_v1"
const schemaEnsureMaxAttempts = 3
const schemaEnsureRetryDelay = 100 * time.Millisecond
const serviceStateStaleAfter = 15 * time.Second
const processedEventWriteMaxAttempts = 3
const processedEventRetryDelay = 25 * time.Millisecond
const processorAllowedLateness = 2 * time.Minute
const tenantMetricShardCount = 32

var eventWindowSizes = []time.Duration{time.Minute, 5 * time.Minute}

func New(ctx context.Context, connectionString string) (*Store, error) {
	return NewWithAdmin(ctx, connectionString, "")
}

func NewWithAdmin(ctx context.Context, connectionString string, adminConnectionString string) (*Store, error) {
	config, err := pgxpool.ParseConfig(connectionString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}
	runtimeRole := strings.TrimSpace(config.ConnConfig.User)
	if runtimeRole == "" {
		return nil, fmt.Errorf("postgres runtime user is required")
	}

	config.MaxConns = 10
	if config.ConnConfig.RuntimeParams == nil {
		config.ConnConfig.RuntimeParams = map[string]string{}
	}
	config.ConnConfig.RuntimeParams["app.role"] = string(auth.RoleService)

	store := &Store{runtimeRole: runtimeRole}
	if strings.TrimSpace(adminConnectionString) != "" {
		adminConfig, err := pgxpool.ParseConfig(adminConnectionString)
		if err != nil {
			return nil, fmt.Errorf("parse postgres admin config: %w", err)
		}
		adminConfig.MaxConns = 2

		adminPool, err := pgxpool.NewWithConfig(ctx, adminConfig)
		if err != nil {
			return nil, fmt.Errorf("connect postgres admin: %w", err)
		}
		if err := store.ensureRuntimeRole(ctx, adminPool, runtimeRole, config.ConnConfig.Password, config.ConnConfig.Database); err != nil {
			adminPool.Close()
			return nil, err
		}
		if err := store.ensureSchema(ctx, adminPool); err != nil {
			adminPool.Close()
			return nil, err
		}
		adminPool.Close()
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	store.pool = pool
	if strings.TrimSpace(adminConnectionString) == "" {
		if err := store.ensureSchema(ctx, pool); err != nil {
			pool.Close()
			return nil, err
		}
	}
	return store, nil
}

func (s *Store) Close() {
	if s.pool != nil {
		s.pool.Close()
	}
}

func (s *Store) EnsureSchema(ctx context.Context) error {
	return s.ensureSchema(ctx, s.pool)
}

func (s *Store) ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	for attempt := 1; attempt <= schemaEnsureMaxAttempts; attempt++ {
		err := s.ensureSchemaOnce(ctx, pool)
		if err == nil {
			return nil
		}
		if !isRetryablePostgresError(err) || attempt == schemaEnsureMaxAttempts {
			return err
		}

		timer := time.NewTimer(time.Duration(attempt) * schemaEnsureRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return fmt.Errorf("ensure schema: exhausted retry budget")
}

func (s *Store) ensureSchemaOnce(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
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

	if _, err := conn.Exec(ctx, `
CREATE TABLE IF NOT EXISTS schema_migrations (
    version TEXT PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`); err != nil {
		return fmt.Errorf("ensure schema migrations table: %w", err)
	}

	var applied bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, schemaVersion).Scan(&applied); err != nil {
		return fmt.Errorf("check schema migration marker: %w", err)
	}
	if applied {
		return nil
	}

	if _, err := conn.Exec(ctx, s.schemaSQL()); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	if _, err := conn.Exec(ctx, `INSERT INTO schema_migrations (version) VALUES ($1) ON CONFLICT (version) DO NOTHING`, schemaVersion); err != nil {
		return fmt.Errorf("record schema migration marker: %w", err)
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
	result, err := s.RecordProcessedEventBatch(ctx, []ProcessedEventInput{{Event: event}})
	if err != nil {
		return false, err
	}
	return result.RecordedCount == 1, nil
}

func (s *Store) RecordProcessedEventBatch(ctx context.Context, inputs []ProcessedEventInput) (ProcessedEventBatchResult, error) {
	if len(inputs) == 0 {
		return ProcessedEventBatchResult{}, nil
	}
	for attempt := 1; attempt <= processedEventWriteMaxAttempts; attempt++ {
		result, err := s.recordProcessedEventBatchOnce(ctx, inputs)
		if err == nil {
			return result, nil
		}
		if ctx.Err() != nil {
			return ProcessedEventBatchResult{}, ctx.Err()
		}
		if !isRetryableProcessedEventError(err) || attempt == processedEventWriteMaxAttempts {
			return ProcessedEventBatchResult{}, fmt.Errorf("record processed event batch: %w", err)
		}

		timer := time.NewTimer(time.Duration(attempt) * processedEventRetryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ProcessedEventBatchResult{}, ctx.Err()
		case <-timer.C:
		}
	}

	return ProcessedEventBatchResult{}, fmt.Errorf("record processed event batch: exhausted retry budget")
}

func (s *Store) recordProcessedEventBatchOnce(ctx context.Context, inputs []ProcessedEventInput) (ProcessedEventBatchResult, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return ProcessedEventBatchResult{}, fmt.Errorf("begin transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback(context.Background())
		}
	}()

	result := ProcessedEventBatchResult{}
	eventIDs := make([]string, 0, len(inputs))
	tenantIDs := make([]string, 0, len(inputs))
	processedAt := make([]time.Time, 0, len(inputs))
	now := time.Now().UTC()
	for _, input := range inputs {
		eventIDs = append(eventIDs, input.Event.EventID)
		tenantIDs = append(tenantIDs, input.Event.TenantID)
		processedAt = append(processedAt, now)
	}

	stageStart := time.Now()
	rows, err := tx.Query(
		ctx,
		`INSERT INTO processed_events (event_id, tenant_id, processed_at)
		 SELECT event_id, tenant_id, processed_at
		 FROM unnest($1::text[], $2::text[], $3::timestamptz[]) AS rows(event_id, tenant_id, processed_at)
		 ON CONFLICT (event_id) DO NOTHING
		 RETURNING event_id`,
		eventIDs,
		tenantIDs,
		processedAt,
	)
	if err != nil {
		return ProcessedEventBatchResult{}, fmt.Errorf("claim processed event batch: %w", err)
	}
	claimedRemaining := map[string]int64{}
	for rows.Next() {
		var eventID string
		if err := rows.Scan(&eventID); err != nil {
			rows.Close()
			return ProcessedEventBatchResult{}, fmt.Errorf("scan processed event claim: %w", err)
		}
		claimedRemaining[eventID]++
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return ProcessedEventBatchResult{}, fmt.Errorf("claim processed event rows: %w", err)
	}
	rows.Close()
	result.StageDurations.DedupClaimMS = durationMS(time.Since(stageStart))

	tenantAggregates := map[tenantMetricKey]metricAggregate{}
	sourceAggregates := map[sourceMetricKey]metricAggregate{}
	windowAggregates := map[windowMetricKey]metricAggregate{}

	for _, input := range inputs {
		if claimedRemaining[input.Event.EventID] <= 0 {
			result.DuplicateCount++
			continue
		}
		claimedRemaining[input.Event.EventID]--

		if input.Late {
			result.LateCount++
			continue
		}

		eventTime := input.Event.Timestamp.UTC()
		okCount, warnCount, errorCount := statusCounts(input.Event.Status)
		aggregate := metricAggregate{
			EventsCount: 1,
			OKCount:     okCount,
			WarnCount:   warnCount,
			ErrorCount:  errorCount,
			ValueSum:    input.Event.Value,
			LastEventAt: eventTime,
		}
		addAggregate(tenantAggregates, tenantMetricKey{
			BucketStart: eventTime.Truncate(10 * time.Second),
			TenantID:    input.Event.TenantID,
			ShardID:     tenantMetricShardID(input.Event.TenantID, input.Event.SourceID),
		}, aggregate)
		addAggregate(sourceAggregates, sourceMetricKey{
			TenantID: input.Event.TenantID,
			SourceID: input.Event.SourceID,
		}, aggregate)
		for _, windowSize := range eventWindowSizes {
			addAggregate(windowAggregates, windowMetricKey{
				WindowSizeSeconds: int(windowSize.Seconds()),
				WindowStart:       eventTime.Truncate(windowSize),
				TenantID:          input.Event.TenantID,
				SourceID:          input.Event.SourceID,
			}, aggregate)
		}
		result.RecordedCount++
	}

	stageStart = time.Now()
	tenantKeys := make([]tenantMetricKey, 0, len(tenantAggregates))
	for key := range tenantAggregates {
		tenantKeys = append(tenantKeys, key)
	}
	sort.Slice(tenantKeys, func(i, j int) bool {
		if !tenantKeys[i].BucketStart.Equal(tenantKeys[j].BucketStart) {
			return tenantKeys[i].BucketStart.Before(tenantKeys[j].BucketStart)
		}
		if tenantKeys[i].TenantID != tenantKeys[j].TenantID {
			return tenantKeys[i].TenantID < tenantKeys[j].TenantID
		}
		return tenantKeys[i].ShardID < tenantKeys[j].ShardID
	})
	if err := upsertTenantMetricsBatch(ctx, tx, tenantKeys, tenantAggregates); err != nil {
		return ProcessedEventBatchResult{}, err
	}
	result.StageDurations.TenantAggregateMS = durationMS(time.Since(stageStart))

	stageStart = time.Now()
	sourceKeys := make([]sourceMetricKey, 0, len(sourceAggregates))
	for key := range sourceAggregates {
		sourceKeys = append(sourceKeys, key)
	}
	sort.Slice(sourceKeys, func(i, j int) bool {
		if sourceKeys[i].TenantID == sourceKeys[j].TenantID {
			return sourceKeys[i].SourceID < sourceKeys[j].SourceID
		}
		return sourceKeys[i].TenantID < sourceKeys[j].TenantID
	})
	if err := upsertSourceMetricsBatch(ctx, tx, sourceKeys, sourceAggregates); err != nil {
		return ProcessedEventBatchResult{}, err
	}
	result.StageDurations.SourceAggregateMS = durationMS(time.Since(stageStart))

	stageStart = time.Now()
	windowKeys := make([]windowMetricKey, 0, len(windowAggregates))
	for key := range windowAggregates {
		windowKeys = append(windowKeys, key)
	}
	sort.Slice(windowKeys, func(i, j int) bool {
		if windowKeys[i].WindowSizeSeconds != windowKeys[j].WindowSizeSeconds {
			return windowKeys[i].WindowSizeSeconds < windowKeys[j].WindowSizeSeconds
		}
		if !windowKeys[i].WindowStart.Equal(windowKeys[j].WindowStart) {
			return windowKeys[i].WindowStart.Before(windowKeys[j].WindowStart)
		}
		if windowKeys[i].TenantID != windowKeys[j].TenantID {
			return windowKeys[i].TenantID < windowKeys[j].TenantID
		}
		return windowKeys[i].SourceID < windowKeys[j].SourceID
	})
	if err := upsertEventWindowsBatch(ctx, tx, windowKeys, windowAggregates); err != nil {
		return ProcessedEventBatchResult{}, err
	}
	result.StageDurations.WindowAggregateMS = durationMS(time.Since(stageStart))

	stageStart = time.Now()
	if err := tx.Commit(ctx); err != nil {
		return ProcessedEventBatchResult{}, fmt.Errorf("commit processed event transaction: %w", err)
	}
	committed = true
	result.StageDurations.CommitMS = durationMS(time.Since(stageStart))
	return result, nil
}

func isRetryableProcessedEventError(err error) bool {
	return isRetryablePostgresError(err)
}

func isRetryablePostgresError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return false
	}

	switch pgErr.Code {
	case "40P01", "40001":
		return true
	default:
		return false
	}
}

type tenantMetricKey struct {
	BucketStart time.Time
	TenantID    string
	ShardID     int32
}

type sourceMetricKey struct {
	TenantID string
	SourceID string
}

type windowMetricKey struct {
	WindowSizeSeconds int
	WindowStart       time.Time
	TenantID          string
	SourceID          string
}

type metricAggregate struct {
	EventsCount int64
	OKCount     int64
	WarnCount   int64
	ErrorCount  int64
	ValueSum    float64
	LastEventAt time.Time
}

func addAggregate[K comparable](target map[K]metricAggregate, key K, value metricAggregate) {
	current := target[key]
	current.EventsCount += value.EventsCount
	current.OKCount += value.OKCount
	current.WarnCount += value.WarnCount
	current.ErrorCount += value.ErrorCount
	current.ValueSum += value.ValueSum
	if current.LastEventAt.IsZero() || value.LastEventAt.After(current.LastEventAt) {
		current.LastEventAt = value.LastEventAt
	}
	target[key] = current
}

func tenantMetricShardID(tenantID string, sourceID string) int32 {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(tenantID))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write([]byte(sourceID))
	return int32(hash.Sum32() % tenantMetricShardCount)
}

func upsertTenantMetricsBatch(ctx context.Context, tx pgx.Tx, keys []tenantMetricKey, aggregates map[tenantMetricKey]metricAggregate) error {
	if len(keys) == 0 {
		return nil
	}

	bucketStarts := make([]time.Time, 0, len(keys))
	tenantIDs := make([]string, 0, len(keys))
	shardIDs := make([]int32, 0, len(keys))
	eventsCounts := make([]int64, 0, len(keys))
	okCounts := make([]int64, 0, len(keys))
	warnCounts := make([]int64, 0, len(keys))
	errorCounts := make([]int64, 0, len(keys))
	valueSums := make([]float64, 0, len(keys))
	lastEventAts := make([]time.Time, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		bucketStarts = append(bucketStarts, key.BucketStart)
		tenantIDs = append(tenantIDs, key.TenantID)
		shardIDs = append(shardIDs, key.ShardID)
		eventsCounts = append(eventsCounts, aggregate.EventsCount)
		okCounts = append(okCounts, aggregate.OKCount)
		warnCounts = append(warnCounts, aggregate.WarnCount)
		errorCounts = append(errorCounts, aggregate.ErrorCount)
		valueSums = append(valueSums, aggregate.ValueSum)
		lastEventAts = append(lastEventAts, aggregate.LastEventAt)
	}

	if _, err := tx.Exec(
		ctx,
		`INSERT INTO tenant_metric_shards (
		     bucket_start, tenant_id, shard_id, events_count, ok_count, warn_count, error_count, value_sum, last_event_at
		 )
		 SELECT bucket_start, tenant_id, shard_id, events_count, ok_count, warn_count, error_count, value_sum, last_event_at
		 FROM unnest(
		     $1::timestamptz[], $2::text[], $3::integer[], $4::bigint[], $5::bigint[],
		     $6::bigint[], $7::bigint[], $8::double precision[], $9::timestamptz[]
		 ) AS rows(bucket_start, tenant_id, shard_id, events_count, ok_count, warn_count, error_count, value_sum, last_event_at)
		 ON CONFLICT (bucket_start, tenant_id, shard_id)
		 DO UPDATE SET
		     events_count = tenant_metric_shards.events_count + EXCLUDED.events_count,
		     ok_count = tenant_metric_shards.ok_count + EXCLUDED.ok_count,
		     warn_count = tenant_metric_shards.warn_count + EXCLUDED.warn_count,
		     error_count = tenant_metric_shards.error_count + EXCLUDED.error_count,
		     value_sum = tenant_metric_shards.value_sum + EXCLUDED.value_sum,
		     last_event_at = GREATEST(tenant_metric_shards.last_event_at, EXCLUDED.last_event_at)`,
		bucketStarts,
		tenantIDs,
		shardIDs,
		eventsCounts,
		okCounts,
		warnCounts,
		errorCounts,
		valueSums,
		lastEventAts,
	); err != nil {
		return fmt.Errorf("upsert tenant metrics: %w", err)
	}
	return nil
}

func upsertSourceMetricsBatch(ctx context.Context, tx pgx.Tx, keys []sourceMetricKey, aggregates map[sourceMetricKey]metricAggregate) error {
	if len(keys) == 0 {
		return nil
	}

	tenantIDs := make([]string, 0, len(keys))
	sourceIDs := make([]string, 0, len(keys))
	eventsCounts := make([]int64, 0, len(keys))
	lastEventAts := make([]time.Time, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		tenantIDs = append(tenantIDs, key.TenantID)
		sourceIDs = append(sourceIDs, key.SourceID)
		eventsCounts = append(eventsCounts, aggregate.EventsCount)
		lastEventAts = append(lastEventAts, aggregate.LastEventAt)
	}

	if _, err := tx.Exec(
		ctx,
		`INSERT INTO source_metrics (tenant_id, source_id, events_count, last_event_at)
		 SELECT tenant_id, source_id, events_count, last_event_at
		 FROM unnest(
		     $1::text[], $2::text[], $3::bigint[], $4::timestamptz[]
		 ) AS rows(tenant_id, source_id, events_count, last_event_at)
		 ON CONFLICT (tenant_id, source_id)
		 DO UPDATE SET
		     events_count = source_metrics.events_count + EXCLUDED.events_count,
		     last_event_at = GREATEST(source_metrics.last_event_at, EXCLUDED.last_event_at)`,
		tenantIDs,
		sourceIDs,
		eventsCounts,
		lastEventAts,
	); err != nil {
		return fmt.Errorf("upsert source metrics: %w", err)
	}
	return nil
}

func upsertEventWindowsBatch(ctx context.Context, tx pgx.Tx, keys []windowMetricKey, aggregates map[windowMetricKey]metricAggregate) error {
	if len(keys) == 0 {
		return nil
	}

	windowSizeSeconds := make([]int32, 0, len(keys))
	windowStarts := make([]time.Time, 0, len(keys))
	tenantIDs := make([]string, 0, len(keys))
	sourceIDs := make([]string, 0, len(keys))
	eventsCounts := make([]int64, 0, len(keys))
	okCounts := make([]int64, 0, len(keys))
	warnCounts := make([]int64, 0, len(keys))
	errorCounts := make([]int64, 0, len(keys))
	valueSums := make([]float64, 0, len(keys))
	maxEventAts := make([]time.Time, 0, len(keys))
	for _, key := range keys {
		aggregate := aggregates[key]
		windowSizeSeconds = append(windowSizeSeconds, int32(key.WindowSizeSeconds))
		windowStarts = append(windowStarts, key.WindowStart)
		tenantIDs = append(tenantIDs, key.TenantID)
		sourceIDs = append(sourceIDs, key.SourceID)
		eventsCounts = append(eventsCounts, aggregate.EventsCount)
		okCounts = append(okCounts, aggregate.OKCount)
		warnCounts = append(warnCounts, aggregate.WarnCount)
		errorCounts = append(errorCounts, aggregate.ErrorCount)
		valueSums = append(valueSums, aggregate.ValueSum)
		maxEventAts = append(maxEventAts, aggregate.LastEventAt)
	}

	if _, err := tx.Exec(
		ctx,
		`INSERT INTO event_windows (
		     window_size_seconds, window_start, tenant_id, source_id,
		     events_count, ok_count, warn_count, error_count, value_sum, max_event_at
		 )
		 SELECT
		     window_size_seconds, window_start, tenant_id, source_id,
		     events_count, ok_count, warn_count, error_count, value_sum, max_event_at
		 FROM unnest(
		     $1::integer[], $2::timestamptz[], $3::text[], $4::text[],
		     $5::bigint[], $6::bigint[], $7::bigint[], $8::bigint[],
		     $9::double precision[], $10::timestamptz[]
		 ) AS rows(
		     window_size_seconds, window_start, tenant_id, source_id,
		     events_count, ok_count, warn_count, error_count, value_sum, max_event_at
		 )
		 ON CONFLICT (window_size_seconds, window_start, tenant_id, source_id)
		 DO UPDATE SET
		     events_count = event_windows.events_count + EXCLUDED.events_count,
		     ok_count = event_windows.ok_count + EXCLUDED.ok_count,
		     warn_count = event_windows.warn_count + EXCLUDED.warn_count,
		     error_count = event_windows.error_count + EXCLUDED.error_count,
		     value_sum = event_windows.value_sum + EXCLUDED.value_sum,
		     max_event_at = GREATEST(event_windows.max_event_at, EXCLUDED.max_event_at)`,
		windowSizeSeconds,
		windowStarts,
		tenantIDs,
		sourceIDs,
		eventsCounts,
		okCounts,
		warnCounts,
		errorCounts,
		valueSums,
		maxEventAts,
	); err != nil {
		return fmt.Errorf("upsert event windows: %w", err)
	}
	return nil
}

func durationMS(value time.Duration) float64 {
	return float64(value.Microseconds()) / 1000.0
}

func (s *Store) GetOverview(ctx context.Context) (Overview, error) {
	var overview Overview
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		var processedLastMinute int64
		var errorLastMinute int64
		if err := q.QueryRow(
			ctx,
			`WITH tenant_rollup AS (
			     SELECT bucket_start, tenant_id, events_count, error_count
			     FROM tenant_metrics
			     UNION ALL
			     SELECT bucket_start, tenant_id, events_count, error_count
			     FROM tenant_metric_shards
			 )
			 SELECT
			     COALESCE(SUM(events_count), 0),
			     COALESCE(SUM(error_count), 0)
			 FROM tenant_rollup
			 WHERE bucket_start >= NOW() - INTERVAL '1 minute'`,
		).Scan(&processedLastMinute, &errorLastMinute); err != nil {
			return fmt.Errorf("query minute aggregates: %w", err)
		}

		var processedTotal int64
		if err := q.QueryRow(
			ctx,
			`SELECT COALESCE(SUM(events_count), 0) FROM source_metrics`,
		).Scan(&processedTotal); err != nil {
			return fmt.Errorf("query processed total: %w", err)
		}

		rejections, err := s.recentRejectionsWithQuerier(ctx, q, 10)
		if err != nil {
			return err
		}

		states, err := s.getStatesWithQuerier(ctx, q)
		if err != nil {
			return err
		}

		overview = Overview{
			GeneratedAt:          time.Now().UTC(),
			ProcessedTotal:       processedTotal,
			StoredProcessedTotal: processedTotal,
			EventsPerSecond:      float64(processedLastMinute) / 60.0,
			WindowSizes:          []string{time.Minute.String(), (5 * time.Minute).String()},
			AllowedLateness:      processorAllowedLateness.String(),
			PartitionHealth:      []PartitionState{},
			RecentRejections:     ensureRecentRejectionsSlice(rejections),
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
			overview.ProcessorProcessedTotal = processor.ProcessedTotal
			overview.DuplicateTotal = processor.DuplicateTotal
			overview.LateEventTotal = processor.LateEventTotal
			overview.DeadLetterTotal = processor.DeadLetterTotal
			overview.ConsumerLag = processor.ConsumerLag
			overview.ProcessorInstances = processor.InstanceCount
			overview.ActivePartitions = processor.ActivePartitions
			overview.ProcessorInFlight = processor.InFlightMessages
			overview.ProcessingP50MS = processor.ProcessingP50MS
			overview.ProcessingP95MS = processor.ProcessingP95MS
			overview.ProcessingP99MS = processor.ProcessingP99MS
			overview.BatchSizeP95 = processor.BatchSizeP95
			overview.BatchFlushP95MS = processor.BatchFlushP95MS
			overview.PartitionHealth = ensurePartitionStateSlice(processor.Partitions)
			overview.ProcessorLastSeenAt = processor.LastSeenAt
		}

		return nil
	})
	if err != nil {
		return Overview{}, err
	}
	return overview, nil
}

func (s *Store) GetEventWindows(
	ctx context.Context,
	tenantID string,
	sourceID string,
	windowSize time.Duration,
	lookback time.Duration,
) ([]WindowBucket, error) {
	if windowSize <= 0 {
		windowSize = time.Minute
	}
	if lookback <= 0 {
		lookback = 15 * time.Minute
	}
	windowSizeSeconds := int(windowSize.Seconds())

	var buckets []WindowBucket
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		var rows pgx.Rows
		var err error
		if strings.TrimSpace(sourceID) == "" {
			rows, err = q.Query(
				ctx,
				`SELECT
				     window_start,
				     tenant_id,
				     '' AS source_id,
				     COALESCE(SUM(events_count), 0),
				     COALESCE(SUM(ok_count), 0),
				     COALESCE(SUM(warn_count), 0),
				     COALESCE(SUM(error_count), 0),
				     CASE WHEN COALESCE(SUM(events_count), 0) = 0 THEN 0 ELSE COALESCE(SUM(value_sum), 0) / SUM(events_count) END,
				     COALESCE(MAX(max_event_at), window_start),
				     EXTRACT(EPOCH FROM (NOW() - COALESCE(MAX(max_event_at), window_start))) * 1000
				 FROM event_windows
				 WHERE tenant_id = $1
				   AND window_size_seconds = $2
				   AND window_start >= NOW() - $3::interval
				 GROUP BY window_start, tenant_id
				 ORDER BY window_start ASC`,
				tenantID,
				windowSizeSeconds,
				formatInterval(lookback),
			)
		} else {
			rows, err = q.Query(
				ctx,
				`SELECT
				     window_start,
				     tenant_id,
				     source_id,
				     events_count,
				     ok_count,
				     warn_count,
				     error_count,
				     CASE WHEN events_count = 0 THEN 0 ELSE value_sum / events_count END,
				     max_event_at,
				     EXTRACT(EPOCH FROM (NOW() - max_event_at)) * 1000
				 FROM event_windows
				 WHERE tenant_id = $1
				   AND source_id = $2
				   AND window_size_seconds = $3
				   AND window_start >= NOW() - $4::interval
				 ORDER BY window_start ASC`,
				tenantID,
				sourceID,
				windowSizeSeconds,
				formatInterval(lookback),
			)
		}
		if err != nil {
			return fmt.Errorf("query event windows: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var bucket WindowBucket
			if err := rows.Scan(
				&bucket.WindowStart,
				&bucket.TenantID,
				&bucket.SourceID,
				&bucket.EventsCount,
				&bucket.OKCount,
				&bucket.WarnCount,
				&bucket.ErrorCount,
				&bucket.Average,
				&bucket.MaxEventAt,
				&bucket.FreshnessMS,
			); err != nil {
				return fmt.Errorf("scan event window: %w", err)
			}
			bucket.WindowSize = windowSize.String()
			buckets = append(buckets, bucket)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if buckets == nil {
		return []WindowBucket{}, nil
	}
	return buckets, nil
}

func (s *Store) GetTenantSeries(ctx context.Context, tenantID string, window time.Duration) ([]TenantBucket, error) {
	var buckets []TenantBucket
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		rows, err := q.Query(
			ctx,
			`WITH tenant_rollup AS (
			     SELECT bucket_start, tenant_id, events_count, ok_count, warn_count, error_count, value_sum
			     FROM tenant_metrics
			     UNION ALL
			     SELECT bucket_start, tenant_id, events_count, ok_count, warn_count, error_count, value_sum
			     FROM tenant_metric_shards
			 )
			 SELECT bucket_start,
			        COALESCE(SUM(events_count), 0),
			        COALESCE(SUM(ok_count), 0),
			        COALESCE(SUM(warn_count), 0),
			        COALESCE(SUM(error_count), 0),
			        CASE WHEN COALESCE(SUM(events_count), 0) = 0 THEN 0 ELSE COALESCE(SUM(value_sum), 0) / SUM(events_count) END AS average_value
			 FROM tenant_rollup
			 WHERE tenant_id = $1 AND bucket_start >= NOW() - $2::interval
			 GROUP BY bucket_start
			 ORDER BY bucket_start ASC`,
			tenantID,
			formatInterval(window),
		)
		if err != nil {
			return fmt.Errorf("query tenant series: %w", err)
		}
		defer rows.Close()

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
				return fmt.Errorf("scan tenant series: %w", err)
			}
			buckets = append(buckets, bucket)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if buckets == nil {
		return []TenantBucket{}, nil
	}
	return buckets, nil
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

	var sources []SourceMetric
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		rows, err := q.Query(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("query top sources: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var source SourceMetric
			if err := rows.Scan(&source.TenantID, &source.SourceID, &source.Events, &source.LastEvent); err != nil {
				return fmt.Errorf("scan top source: %w", err)
			}
			sources = append(sources, source)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	if sources == nil {
		return []SourceMetric{}, nil
	}
	return sources, nil
}

func (s *Store) RecentRejections(ctx context.Context, limit int) ([]RecentRejection, error) {
	var rejections []RecentRejection
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		var err error
		rejections, err = s.recentRejectionsWithQuerier(ctx, q, limit)
		return err
	})
	if err != nil {
		return nil, err
	}
	if rejections == nil {
		return []RecentRejection{}, nil
	}
	return rejections, nil
}

func (s *Store) recentRejectionsWithQuerier(ctx context.Context, q dbQuerier, limit int) ([]RecentRejection, error) {
	rows, err := q.Query(
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
	var results map[string][]stateEnvelope
	err := s.withScopedQuerier(ctx, func(q dbQuerier) error {
		var innerErr error
		results, innerErr = s.getStatesWithQuerier(ctx, q)
		return innerErr
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

func (s *Store) getStatesWithQuerier(ctx context.Context, q dbQuerier) (map[string][]stateEnvelope, error) {
	rows, err := q.Query(
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

func ensureRecentRejectionsSlice(rejections []RecentRejection) []RecentRejection {
	if rejections == nil {
		return []RecentRejection{}
	}
	return rejections
}

func ensurePartitionStateSlice(partitions []PartitionState) []PartitionState {
	if partitions == nil {
		return []PartitionState{}
	}
	return partitions
}

func formatInterval(value time.Duration) string {
	return fmt.Sprintf("%f seconds", value.Seconds())
}
