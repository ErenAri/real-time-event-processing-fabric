# API Specification

## Endpoint summary

| Service | Method | Path | Auth | Purpose |
| --- | --- | --- | --- | --- |
| `ingest-service` | `POST` | `/api/v1/events` | none in local Compose | Accept one telemetry event |
| `ingest-service` | `POST` | `/api/v1/admin/replay` | admin token | Replay archived events back into Kafka |
| `query-service` | `GET` | `/api/v1/metrics/overview` | none in local Compose | Return platform-wide overview metrics |
| `query-service` | `GET` | `/api/v1/metrics/tenants/{tenantId}` | none in local Compose | Return tenant bucket series |
| `query-service` | `GET` | `/api/v1/metrics/sources/top` | none in local Compose | Return top sources, optionally filtered by tenant |
| `query-service` | `GET` | `/api/v1/metrics/rejections` | none in local Compose | Return latest ingest rejections |
| every Go service | `GET` | `/healthz` | none | Liveness probe |
| every Go service | `GET` | `/readyz` | none | Readiness probe |
| every Go service | `GET` | `/metrics` | none | Prometheus metrics |

## Common conventions

- Payloads are JSON.
- Timestamps are UTC RFC3339 values.
- Error responses use the form:

```json
{
  "error": "message"
}
```

- The local Compose deployment is intentionally open except for the replay endpoint.

## Ingest event

### `POST /api/v1/events`

Accepts one telemetry event and returns `202 Accepted` when the event is validated, archived, and published to Kafka.

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

Successful response:

```json
{
  "status": "accepted",
  "event_id": "tenant_01-sensor_001-42",
  "tenant_id": "tenant_01"
}
```

Typical rejection:

```json
{
  "error": "invalid event payload"
}
```

## Overview query

### `GET /api/v1/metrics/overview`

Returns platform-wide state aggregated from PostgreSQL hot views and recent service snapshots.

Representative response shape:

```json
{
  "generated_at": "2026-04-10T18:29:14Z",
  "accepted_total": 251154,
  "rejected_total": 343,
  "processed_total": 1910754,
  "duplicate_total": 10385,
  "consumer_lag": 0,
  "processor_instances": 3,
  "processor_active_partitions": 3,
  "processor_inflight_messages": 402,
  "events_per_second_last_minute": 558.11,
  "error_rate_last_minute": 0.2447,
  "processing_p50_ms": 2,
  "processing_p95_ms": 6,
  "processing_p99_ms": 11,
  "recent_rejections": []
}
```

## Tenant series

### `GET /api/v1/metrics/tenants/{tenantId}?window=15m`

Returns 10-second tenant buckets for the requested time window.

Response shape:

```json
{
  "tenant_id": "tenant_01",
  "window": "15m0s",
  "series": [
    {
      "bucket_start": "2026-04-10T18:20:00Z",
      "events_count": 245,
      "ok_count": 180,
      "warn_count": 32,
      "error_count": 33,
      "average_value": 72.9
    }
  ]
}
```

## Top sources

### `GET /api/v1/metrics/sources/top?tenantId=tenant_01&limit=8`

Returns the most active sources ordered by cumulative event count.

## Recent rejections

### `GET /api/v1/metrics/rejections?limit=10`

Returns the newest rejection rows recorded by the ingest service.

## Replay

### `POST /api/v1/admin/replay`

This endpoint republishes archived events from the raw archive back into Kafka. It is intended for local operator use only.

Auth headers:

- `X-Admin-Token: pulsestream-dev-admin`
- or `Authorization: Bearer pulsestream-dev-admin`

Request body:

```json
{
  "start_date": "2026-04-10",
  "end_date": "2026-04-10",
  "tenant_id": "tenant_01",
  "limit": 500
}
```

Successful response:

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

## Authentication scope

The local Compose environment is intentionally unauthenticated for the ingest and query endpoints so benchmarking and failure drills remain straightforward. Tenant authentication and authorization are not implemented yet. The replay endpoint is the only local endpoint that is protected.
