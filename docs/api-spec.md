# API Specification

## Endpoint summary

| Service | Method | Path | Auth | Purpose |
| --- | --- | --- | --- | --- |
| `ingest-service` | `POST` | `/api/v1/events` | bearer JWT | Accept one telemetry event |
| `ingest-service` | `POST` | `/api/v1/admin/replay` | admin token or admin bearer JWT | Replay archived events back into Kafka |
| `query-service` | `GET` | `/api/v1/metrics/overview` | bearer JWT | Return platform-wide overview metrics |
| `query-service` | `GET` | `/api/v1/metrics/tenants/{tenantId}` | bearer JWT | Return tenant bucket series |
| `query-service` | `GET` | `/api/v1/metrics/sources/top` | bearer JWT | Return top sources, optionally filtered by tenant |
| `query-service` | `GET` | `/api/v1/metrics/rejections` | bearer JWT | Return latest ingest rejections |
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
  "dead_letter_total": 1,
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

Returns the most active sources ordered by cumulative event count. `tenant_user` tokens can only read their assigned tenant.

## Recent rejections

### `GET /api/v1/metrics/rejections?limit=10`

Returns the newest rejection rows recorded by the ingest service.

## Replay

### `POST /api/v1/admin/replay`

This endpoint republishes archived events from the raw archive back into Kafka. It is intended for local operator recovery and replay drills.

Auth headers:

- `X-Admin-Token: pulsestream-dev-admin`
- or `Authorization: Bearer <admin-jwt>`

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

## Service health and metrics

Each Go service exposes:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`

## Async contract

Kafka message contracts are documented separately from the HTTP APIs:

- [asyncapi.yaml](../asyncapi.yaml) for topic addresses, operations, headers, and examples
- [telemetry-event-v1.schema.json](../schemas/telemetry-event-v1.schema.json) for the accepted telemetry payload
- [dead-letter-record-v1.schema.json](../schemas/dead-letter-record-v1.schema.json) for DLQ payloads

## Authentication scope

The local stack requires JWT bearer tokens for ingest and query endpoints.

- `admin` tokens can query any tenant and call replay endpoints.
- `tenant_user` tokens are restricted to their own `tenant_id`.
- `GET /healthz`, `GET /readyz`, and `GET /metrics` remain unauthenticated.
- The replay endpoint accepts `Authorization: Bearer <admin-jwt>` and still accepts `X-Admin-Token` for local recovery drills.
