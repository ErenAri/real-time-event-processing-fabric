# API Specification

## Endpoint summary

| Service | Method | Path | Auth | Purpose |
| --- | --- | --- | --- | --- |
| `ingest-service` | `POST` | `/api/v1/events` | bearer JWT | Accept one telemetry event |
| `ingest-service` | `POST` | `/api/v1/events/batch` | bearer JWT | Accept a bounded batch of telemetry events |
| `ingest-service` | `POST` | `/api/v1/admin/replay` | admin token or admin bearer JWT | Replay archived events back into Kafka |
| `query-service` | `GET` | `/api/v1/metrics/overview` | bearer JWT | Return platform-wide overview metrics |
| `query-service` | `GET` | `/api/v1/metrics/tenants/{tenantId}` | bearer JWT | Return tenant bucket series |
| `query-service` | `GET` | `/api/v1/metrics/windows` | bearer JWT | Return event-time window aggregates |
| `query-service` | `GET` | `/api/v1/metrics/partitions` | admin bearer JWT | Return processor partition ownership and lag |
| `query-service` | `GET` | `/api/v1/metrics/sources/top` | bearer JWT | Return top sources, optionally filtered by tenant |
| `query-service` | `GET` | `/api/v1/metrics/rejections` | bearer JWT | Return latest ingest rejections |
| `query-service` | `GET` | `/api/v1/evidence/latest` | admin bearer JWT | Return the latest generated benchmark and failure-drill evidence summary |
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

## Ingest event batch

### `POST /api/v1/events/batch`

Accepts a JSON array of telemetry events and returns `202 Accepted` when every event is validated, accepted by the archive step, and published to Kafka. The current maximum batch size is `500` events. Batch ingest is all-or-nothing at the API boundary: if any event fails schema validation or tenant authorization, the request is rejected and no events from that request are intentionally published.

`tenant_user` tokens may only submit events for their assigned `tenant_id`. Admin tokens may submit mixed-tenant batches.

Request body:

```json
[
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
  },
  {
    "schema_version": 1,
    "event_id": "tenant_01-sensor_002-43",
    "tenant_id": "tenant_01",
    "source_id": "sensor_002",
    "event_type": "telemetry",
    "timestamp": "2026-04-10T10:15:01Z",
    "value": 81.7,
    "status": "warn",
    "region": "eu-west",
    "sequence": 43
  }
]
```

Successful response:

```json
{
  "status": "accepted",
  "accepted_count": 2
}
```

## Overview query

### `GET /api/v1/metrics/overview`

Returns platform-wide state aggregated from PostgreSQL hot views and recent service snapshots.

Counter semantics:

- `processed_total` and `stored_processed_total` are persisted hot-view totals from PostgreSQL. They can include previous local runs if Docker volumes are reused.
- `accepted_total`, `rejected_total`, and `processor_processed_total` are current service-process snapshot counters. Use these for same-run dashboard ratios and benchmark smoke checks.

Representative response shape:

```json
{
  "generated_at": "2026-04-10T18:29:14Z",
  "accepted_total": 251154,
  "rejected_total": 343,
  "processed_total": 1910754,
  "stored_processed_total": 1910754,
  "processor_processed_total": 250991,
  "duplicate_total": 10385,
  "late_event_total": 14,
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
  "batch_size_p95": 500,
  "batch_flush_p95_ms": 18,
  "window_sizes": ["1m0s", "5m0s"],
  "allowed_lateness": "2m0s",
  "partition_health": [
    {
      "partition": 0,
      "owner_instance_id": "stream-processor-a1",
      "lag": 0,
      "processed_total": 100244,
      "duplicate_total": 120,
      "late_event_total": 2,
      "inflight_messages": 0,
      "last_offset": 90231,
      "last_seen_at": "2026-04-10T18:29:13Z"
    }
  ],
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

## Event-time windows

### `GET /api/v1/metrics/windows?tenantId=tenant_01&windowSize=1m&lookback=15m`

Returns fixed event-time windows assigned from event timestamps. `tenant_user` tokens can only read their assigned tenant. If `sourceId` is omitted, rows are aggregated across sources for the tenant.

Response shape:

```json
{
  "tenant_id": "tenant_01",
  "source_id": "",
  "window_size": "1m0s",
  "lookback": "15m0s",
  "semantic": "event_time",
  "windows": [
    {
      "window_start": "2026-04-10T18:20:00Z",
      "window_size": "1m0s",
      "tenant_id": "tenant_01",
      "events_count": 1200,
      "ok_count": 1000,
      "warn_count": 150,
      "error_count": 50,
      "average_value": 72.9,
      "max_event_at": "2026-04-10T18:20:59Z",
      "freshness_ms": 2500
    }
  ]
}
```

## Partition health

### `GET /api/v1/metrics/partitions`

Returns active processor partition snapshots. This endpoint is admin-only because it exposes processor instance identifiers and operational topology.

Response shape:

```json
{
  "generated_at": "2026-04-10T18:29:14Z",
  "partitions": [
    {
      "partition": 0,
      "owner_instance_id": "stream-processor-a1",
      "lag": 0,
      "processed_total": 100244,
      "duplicate_total": 120,
      "late_event_total": 2,
      "inflight_messages": 0,
      "last_offset": 90231,
      "last_seen_at": "2026-04-10T18:29:13Z"
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

## Evidence summary

### `GET /api/v1/evidence/latest`

Returns the generated operator evidence summary from `artifacts/evidence/latest.json`. The endpoint is admin-only because the response can expose operational artifacts and failure-drill details. If no evidence summary exists, the endpoint returns a `missing` summary with an empty drill list instead of failing the dashboard.

Representative response shape:

```json
{
  "schema_version": 1,
  "generated_at": "2026-04-17T19:52:42Z",
  "status": "available",
  "artifact_root": "artifacts",
  "benchmark": {
    "artifact": "artifacts/benchmarks/benchmark-20260417-222710.json",
    "started_at_utc": "2026-04-17T19:28:04Z",
    "completed_at_utc": "2026-04-17T19:29:08Z",
    "target_eps": 5000,
    "accepted_eps": 955.91,
    "processed_eps": 329.37,
    "query_p95_ms": 147.13,
    "peak_lag": 10969,
    "post_load_drain_seconds": 999999,
    "producer_count": 4,
    "processor_replicas": 3,
    "summary": "Accepted 955.9 eps and processed 329.4 eps against a 5,000 eps target using 4 producers and 3 processor replicas.",
    "gaps": [
      "Accepted throughput is below 80 percent of target; producer or ingest path is still the bottleneck."
    ],
    "gates": [
      {
        "name": "processed_eps_2000",
        "status": "fail",
        "target": 2000,
        "observed": 329.37,
        "unit": "eps"
      }
    ]
  },
  "failure_drills": [
    {
      "scenario_id": "broker-outage",
      "title": "Broker outage",
      "status": "degraded",
      "artifact": "artifacts/failure-drills/broker-outage-20260423-182640.json",
      "started_at_utc": "2026-04-23T15:27:07Z",
      "completed_at_utc": "2026-04-23T15:28:01Z",
      "result": "Archive accounting gap 38592; accepted traffic recovered: True.",
      "operator_note": "Publish failures and backpressure are explicit, and the processor should remain live while Kafka is unavailable.",
      "remaining_gap": "Batch-path archive accounting is still inconsistent under broker disruption.",
      "metrics": [
        {
          "label": "publish failed",
          "value": "1,608"
        }
      ]
    }
  ]
}
```

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
