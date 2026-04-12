# Data Model

## Telemetry event

```json
{
  "schema_version": 1,
  "event_id": "uuid-or-deterministic-id",
  "tenant_id": "tenant_123",
  "source_id": "sensor_42",
  "event_type": "telemetry",
  "timestamp": "2026-04-08T18:00:00Z",
  "value": 73.4,
  "status": "ok",
  "region": "eu-west",
  "sequence": 104424
}
```

## Event invariants

- `schema_version` is required and currently fixed at `1`
- `event_id` must be unique across the stream
- `tenant_id` and `source_id` are required
- `timestamp` must be UTC and parseable as RFC3339
- `status` must be one of `ok`, `warn`, or `error`
- `value` is numeric so aggregation and thresholding remain straightforward

The formal JSON Schema for this contract lives at [schemas/telemetry-event-v1.schema.json](/C:/Projects/real-time-event-processing-fabric/schemas/telemetry-event-v1.schema.json).
The Kafka topic contract that references this schema lives at [asyncapi.yaml](/C:/Projects/real-time-event-processing-fabric/asyncapi.yaml).

## PostgreSQL tables

### `processed_events`

Tracks the deduplication key. If an `event_id` already exists, the processor treats the message as a duplicate and skips aggregate updates.

## Raw archive

Accepted events are also written to an immutable NDJSON archive partitioned by UTC day:

```text
RAW_ARCHIVE_DIR/
  2026/
    04/
      10/
        events.ndjson
```

Each line stores:

- `archived_at`
- the decoded `event`
- the original `raw_payload`

This gives the system a replayable cold path before cloud object storage is introduced.

### `tenant_metrics`

Stores 10-second aggregate buckets:

- total events
- `ok`, `warn`, and `error` counts
- summed values for average calculations
- last event timestamp

### `source_metrics`

Stores cumulative per-tenant source counters for top-N dashboards.

### `rejection_events`

Stores malformed payloads, validation failures, and publish failures from the ingest service.

### `pulsestream.events.dlq`

Stores processor-side poison messages that were already present in Kafka but could not be decoded or validated by the consumer. Each record includes:

- failure timestamp
- failure reason
- error string
- source topic, partition, and offset
- consumer group
- optional event metadata when it could be recovered
- base64-encoded original payload
- base64-encoded Kafka headers

The formal JSON Schema for this payload lives at [schemas/dead-letter-record-v1.schema.json](/C:/Projects/real-time-event-processing-fabric/schemas/dead-letter-record-v1.schema.json).

### `service_state`

Stores periodic snapshots from ingest, processor, and query processes so the query API can surface heartbeats and cumulative counters without scraping Prometheus directly.

Current key shape:

- `service_name`
- `instance_id`

The query layer ignores stale snapshot rows so restarted or removed replicas do not continue to count as active capacity.

Processor snapshot payloads also carry:

- `dead_letter_total`
- `active_partitions`
- `inflight_messages`
- `processing_p50_ms`
- `processing_p95_ms`
- `processing_p99_ms`
