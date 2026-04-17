package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"pulsestream/internal/auth"
)

type dbQuerier interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (s *Store) withScopedQuerier(ctx context.Context, fn func(dbQuerier) error) error {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal.Role == "" || principal.Role == auth.RoleService {
		return fn(s.pool)
	}

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire scoped postgres connection: %w", err)
	}
	defer conn.Release()

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin scoped postgres transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	if _, err := tx.Exec(ctx, `SELECT set_config('app.role', $1, true)`, string(principal.Role)); err != nil {
		return fmt.Errorf("set scoped postgres role: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT set_config('app.tenant_id', $1, true)`, principal.TenantID); err != nil {
		return fmt.Errorf("set scoped postgres tenant: %w", err)
	}

	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit scoped postgres transaction: %w", err)
	}
	return nil
}

func (s *Store) ensureRuntimeRole(ctx context.Context, pool *pgxpool.Pool, roleName string, password string, database string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire postgres connection for runtime role init: %w", err)
	}
	defer conn.Release()

	roleName = strings.TrimSpace(roleName)
	database = strings.TrimSpace(database)
	if roleName == "" || database == "" {
		return fmt.Errorf("runtime postgres role and database are required")
	}

	var exists bool
	if err := conn.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname = $1)`, roleName).Scan(&exists); err != nil {
		return fmt.Errorf("check runtime postgres role: %w", err)
	}

	var roleSQL string
	if exists {
		roleSQL = fmt.Sprintf(`ALTER ROLE %s LOGIN PASSWORD %s`, quoteIdentifier(roleName), quoteLiteral(password))
	} else {
		roleSQL = fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD %s`, quoteIdentifier(roleName), quoteLiteral(password))
	}
	if _, err := conn.Exec(ctx, roleSQL); err != nil {
		return fmt.Errorf("ensure runtime postgres role: %w", err)
	}

	grantSQL := fmt.Sprintf(`GRANT CONNECT ON DATABASE %s TO %s`, quoteIdentifier(database), quoteIdentifier(roleName))
	if _, err := conn.Exec(ctx, grantSQL); err != nil {
		return fmt.Errorf("grant runtime postgres role access: %w", err)
	}
	return nil
}

func (s *Store) schemaSQL() string {
	role := quoteIdentifier(s.runtimeRole)
	return fmt.Sprintf(`
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

CREATE TABLE IF NOT EXISTS event_windows (
    window_size_seconds INTEGER NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,
    tenant_id TEXT NOT NULL,
    source_id TEXT NOT NULL,
    events_count BIGINT NOT NULL DEFAULT 0,
    ok_count BIGINT NOT NULL DEFAULT 0,
    warn_count BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    value_sum DOUBLE PRECISION NOT NULL DEFAULT 0,
    max_event_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (window_size_seconds, window_start, tenant_id, source_id)
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

CREATE INDEX IF NOT EXISTS idx_event_windows_tenant_window
    ON event_windows (tenant_id, window_size_seconds, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_event_windows_source_window
    ON event_windows (tenant_id, source_id, window_size_seconds, window_start DESC);

CREATE INDEX IF NOT EXISTS idx_rejection_events_created_at
    ON rejection_events (created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_service_state_service_instance
    ON service_state (service_name, instance_id);

GRANT USAGE ON SCHEMA public TO %s;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO %s;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO %s;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT USAGE, SELECT ON SEQUENCES TO %s;

ALTER TABLE processed_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE processed_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS processed_events_access ON processed_events;
CREATE POLICY processed_events_access ON processed_events
    FOR ALL TO %s
    USING (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    )
    WITH CHECK (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    );

ALTER TABLE tenant_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE tenant_metrics FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_metrics_access ON tenant_metrics;
CREATE POLICY tenant_metrics_access ON tenant_metrics
    FOR ALL TO %s
    USING (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    )
    WITH CHECK (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    );

ALTER TABLE source_metrics ENABLE ROW LEVEL SECURITY;
ALTER TABLE source_metrics FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS source_metrics_access ON source_metrics;
CREATE POLICY source_metrics_access ON source_metrics
    FOR ALL TO %s
    USING (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    )
    WITH CHECK (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    );

ALTER TABLE event_windows ENABLE ROW LEVEL SECURITY;
ALTER TABLE event_windows FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS event_windows_access ON event_windows;
CREATE POLICY event_windows_access ON event_windows
    FOR ALL TO %s
    USING (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    )
    WITH CHECK (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    );

ALTER TABLE rejection_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE rejection_events FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rejection_events_access ON rejection_events;
CREATE POLICY rejection_events_access ON rejection_events
    FOR ALL TO %s
    USING (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    )
    WITH CHECK (
        current_setting('app.role', true) IN ('service', 'admin')
        OR tenant_id = current_setting('app.tenant_id', true)
    );

ALTER TABLE service_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE service_state FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS service_state_access ON service_state;
CREATE POLICY service_state_access ON service_state
    FOR ALL TO %s
    USING (current_setting('app.role', true) IN ('service', 'admin'))
    WITH CHECK (current_setting('app.role', true) IN ('service', 'admin'));`,
		role,
		role,
		role,
		role,
		role,
		role,
		role,
		role,
		role,
		role,
		role,
	)
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}
