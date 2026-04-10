# API Spec

## Event ingest

### `POST /api/v1/events`

Accepts a single telemetry event.

Request body:

```json
{
  "schema_version": 1,
  "event_id": "tenant_01-sensor_001-42",
  "tenant_id": "tenant_01",
  "source_id": "sensor_001",
  "event_type": "telemetry",
  "timestamp": "2026-04-10T10:15:00Z",
  "value": 73.4,
  "status": "ok",
  "region": "eu-west",
  "sequence": 42
}
```

Response `202 Accepted`:

```json
{
  "status": "accepted",
  "event_id": "tenant_01-sensor_001-42",
  "tenant_id": "tenant_01"
}
```

Response `400 Bad Request`:

```json
{
  "error": "invalid event payload"
}
```

## Query service

### `GET /api/v1/metrics/overview`

Returns platform-wide throughput, lag, latency, and recent rejection information.

### `GET /api/v1/metrics/tenants/{tenantId}?window=15m`

Returns 10-second buckets for the requested tenant.

### `GET /api/v1/metrics/sources/top?tenantId=tenant_01&limit=8`

Returns the most active sources, optionally filtered to a single tenant.

### `GET /api/v1/metrics/rejections?limit=10`

Returns the newest ingest rejections captured in PostgreSQL.

## Admin replay

### `POST /api/v1/admin/replay`

Local admin endpoint on the ingest service. Requires `X-Admin-Token` or `Authorization: Bearer <token>`.

Request body:

```json
{
  "start_date": "2026-04-10",
  "end_date": "2026-04-10",
  "tenant_id": "tenant_01",
  "limit": 500
}
```

Response `200 OK`:

```json
{
  "status": "completed",
  "replay": {
    "start_date": "2026-04-10T00:00:00Z",
    "end_date": "2026-04-10T00:00:00Z",
    "tenant_id": "tenant_01",
    "files_read": 1,
    "scanned": 500,
    "skipped": 0,
    "replayed": 500,
    "completed_at": "2026-04-10T15:30:00Z"
  }
}
```

## Service health and metrics

Each Go service exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

## Auth

The MVP is intentionally unauthenticated inside the local Compose environment. JWT auth and tenant-scoped authorization are reserved for the next gate after throughput and failure evidence are stable.

The replay endpoint is the one exception: it uses a local shared admin token so replay is not anonymously exposed in the dev environment.
