# Architecture

## Objective

PulseStream demonstrates a focused streaming architecture: high-volume event ingress, near-real-time aggregation, hot operational reads, and observable failure behavior. The MVP is intentionally narrow so the benchmark and recovery evidence stays credible.

## Components

- `producer-simulator`: emits synthetic telemetry at configurable rates, including duplicates, malformed payloads, and burst traffic
- `ingest-service`: validates event payloads, records rejections, publishes accepted events to Kafka, and emits ingest metrics
- `raw archive`: immutable NDJSON event log written by ingest-service before broker publish, used for replay and backfills
- `stream-processor`: consumes Kafka partitions, processes partitions in parallel while preserving per-partition ordering, deduplicates by `event_id`, dead-letters poison messages that are already in Kafka, computes hot aggregates, and stores per-instance service state snapshots
- `query-service`: exposes low-latency operational APIs for dashboard reads
- `PostgreSQL`: hot store for aggregate buckets, source counters, rejection history, and service state
- `Kafka`: durable broker between write and processing paths, with a dedicated DLQ topic for processor-side poison messages
- `dashboard`: React UI for throughput, lag, latency, and rejection visibility
- `Prometheus` and `Grafana`: metrics collection and local operational visibility
- `asyncapi.yaml` plus JSON Schemas: checked-in asynchronous contract for Kafka topics, headers, and payloads

## Data flow

### Write path

`producer-simulator -> ingest-service -> raw archive + Kafka -> stream-processor -> PostgreSQL`

Poison-message path:

`Kafka -> stream-processor -> pulsestream.events.dlq`

### Read path

`dashboard -> query-service -> PostgreSQL`

## Design decisions

- Kafka over a cloud-managed broker first: stronger local reproducibility and better control over failure testing
- Event Hubs compatibility through the Kafka protocol: the same Go services can target Azure by switching Kafka transport settings to `SASL_SSL` and `PLAIN`
- PostgreSQL as the only hot store in MVP: simpler than Redis plus Postgres while still supporting aggregate reads cleanly
- Idempotent at-least-once processing: `processed_events.event_id` acts as the duplicate guard without the complexity of exactly-once semantics
- Partition-parallel processing in one processor instance: keeps ordering within a partition but allows different Kafka partitions to make progress concurrently
- Instance-aware processor state aggregation: each processor replica writes its own heartbeat row and the query layer aggregates live replicas instead of assuming a single process
- Fail-closed poison-message handling: processor-side decode and validation failures are written to a DLQ topic before the source offset is committed
- Immutable raw archive before broker publish: keeps a replayable event history even if downstream processing needs to be rebuilt
- AsyncAPI plus JSON Schema contract governance: Kafka topics and payloads are versioned in source control and validated in CI
- Docker Compose before Kubernetes: faster iteration and easier benchmark reproducibility on a single developer machine

## Hot views

- `tenant_metrics`: 10-second time buckets used for rolling throughput and status charts
- `source_metrics`: cumulative source activity for top-N reads
- `rejection_events`: recent ingest failures for operator visibility
- `service_state`: lightweight snapshots keyed by `service_name + instance_id`, with stale rows ignored by the query layer so dead replicas do not inflate live totals

Processor snapshots currently include:

- processed event count
- duplicate discard count
- dead-letter count
- consumer lag
- active partitions
- in-flight message count
- p50, p95, and p99 processing latency

## Observability

- Prometheus metrics from every Go service and the simulator, discovered from Docker so scaled processor replicas are scraped automatically
- JSON structured logs with request timing and failure reasons
- OpenTelemetry HTTP instrumentation is wired in, with trace export disabled by default for local throughput runs and `stdout` export available when explicitly enabled
- Alerting rules for processor outage, elevated lag, and rejection bursts

## Replay path

`admin client -> ingest-service replay endpoint -> raw archive scan -> Kafka -> stream-processor -> PostgreSQL`

## Azure deployment path

The first Azure deployment variant uses:

- Azure Event Hubs as the Kafka endpoint
- Azure Blob Storage for raw archive durability and replay scanning
- Azure Container Apps for `ingest-service`, `query-service`, and `stream-processor`
- Azure Log Analytics through the Container Apps environment
- system-assigned managed identity on `ingest-service` for Blob access
- existing PostgreSQL and Event Hubs credentials injected as Container Apps secrets

This path now provides a durable Azure replay archive. The main remaining Azure gaps are dashboard deployment parity, infrastructure provisioning for the backing services, and published Azure benchmark evidence.
