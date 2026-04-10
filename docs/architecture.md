# Architecture

## Objective

PulseStream demonstrates a focused streaming architecture: high-volume event ingress, near-real-time aggregation, hot operational reads, and observable failure behavior. The MVP is intentionally narrow so the benchmark and recovery evidence stays credible.

## Components

- `producer-simulator`: emits synthetic telemetry at configurable rates, including duplicates, malformed payloads, and burst traffic
- `ingest-service`: validates event payloads, records rejections, publishes accepted events to Kafka, and emits ingest metrics
- `raw archive`: immutable NDJSON event log written by ingest-service before broker publish, used for replay and backfills
- `stream-processor`: consumes Kafka partitions, processes partitions in parallel while preserving per-partition ordering, deduplicates by `event_id`, computes hot aggregates, and stores per-instance service state snapshots
- `query-service`: exposes low-latency operational APIs for dashboard reads
- `PostgreSQL`: hot store for aggregate buckets, source counters, rejection history, and service state
- `Kafka`: durable broker between write and processing paths
- `dashboard`: React UI for throughput, lag, latency, and rejection visibility
- `Prometheus` and `Grafana`: metrics collection and local operational visibility

## Data flow

### Write path

`producer-simulator -> ingest-service -> raw archive + Kafka -> stream-processor -> PostgreSQL`

### Read path

`dashboard -> query-service -> PostgreSQL`

## Design decisions

- Kafka over a cloud-managed broker first: stronger local reproducibility and better control over failure testing
- PostgreSQL as the only hot store in MVP: simpler than Redis plus Postgres while still supporting aggregate reads cleanly
- Idempotent at-least-once processing: `processed_events.event_id` acts as the duplicate guard without the complexity of exactly-once semantics
- Partition-parallel processing in one processor instance: keeps ordering within a partition but allows different Kafka partitions to make progress concurrently
- Instance-aware processor state aggregation: each processor replica writes its own heartbeat row and the query layer aggregates live replicas instead of assuming a single process
- Immutable raw archive before broker publish: keeps a replayable event history even if downstream processing needs to be rebuilt
- Docker Compose before Kubernetes: faster iteration and easier benchmark reproducibility on a single developer machine

## Hot views

- `tenant_metrics`: 10-second time buckets used for rolling throughput and status charts
- `source_metrics`: cumulative source activity for top-N reads
- `rejection_events`: recent ingest failures for operator visibility
- `service_state`: lightweight snapshots keyed by `service_name + instance_id`, with stale rows ignored by the query layer so dead replicas do not inflate live totals

## Observability

- Prometheus metrics from every Go service and the simulator, discovered from Docker so scaled processor replicas are scraped automatically
- JSON structured logs with request timing and failure reasons
- OpenTelemetry HTTP instrumentation is wired in, with trace export disabled by default for local throughput runs and `stdout` export available when explicitly enabled
- Alerting rules for processor outage, elevated lag, and rejection bursts

## Replay path

`admin client -> ingest-service replay endpoint -> raw archive scan -> Kafka -> stream-processor -> PostgreSQL`
