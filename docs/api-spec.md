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

Representative response:

```json
{
  "accepted_total": 471893,
  "rejected_total": 949,
  "processed_total": 6206185,
  "duplicate_total": 0,
  "dead_letter_total": 1,
  "consumer_lag": 0,
  "processor_instances": 1,
  "processor_active_partitions": 1,
  "processor_inflight_messages": 0,
  "processing_p95_ms": 0,
  "recent_rejections": [
    {
      "id": 120911,
      "reason": "decode_failed",
      "created_at": "2026-04-11T15:09:06.031708Z"
    }
  ]
}
```

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

## Async contract

Kafka message contracts are documented separately from the HTTP APIs:

- [asyncapi.yaml](/C:/Projects/real-time-event-processing-fabric/asyncapi.yaml) for topic addresses, operations, headers, and examples
- [telemetry-event-v1.schema.json](/C:/Projects/real-time-event-processing-fabric/schemas/telemetry-event-v1.schema.json) for the accepted telemetry payload
- [dead-letter-record-v1.schema.json](/C:/Projects/real-time-event-processing-fabric/schemas/dead-letter-record-v1.schema.json) for DLQ payloads

## Auth

The local stack requires JWT bearer tokens for ingest and query endpoints.

- `admin` tokens can query any tenant and call replay endpoints
- `tenant_user` tokens are restricted to their own `tenant_id`
- `GET /healthz`, `GET /readyz`, and `GET /metrics` remain unauthenticated

The replay endpoint accepts `Authorization: Bearer <token>` for admin JWTs and still accepts `X-Admin-Token` for local recovery drills.
