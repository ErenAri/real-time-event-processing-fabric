# Architecture

## Scope

PulseStream is a local-first event analytics platform that emphasizes distributed-systems concerns over product breadth. The current implementation focuses on ingestion, streaming aggregation, replay, observability, tenant isolation, and measurable failure behavior.

## System diagram

```mermaid
flowchart LR
    subgraph Load
        PS[producer-simulator]
    end

    subgraph Write path
        IS[ingest-service]
        RA[(raw archive)]
        K[(Kafka)]
        DLQ[(Kafka DLQ)]
    end

    subgraph Processing
        SP1[processor replica 1]
        SP2[processor replica 2]
        SPN[processor replica N]
    end

    subgraph State
        DB[(PostgreSQL)]
    end

    subgraph Read path
        QS[query-service]
        UI[dashboard]
    end

    subgraph Telemetry
        PM[Prometheus]
        GF[Grafana]
    end

    PS --> IS
    IS --> RA
    IS --> K
    K --> SP1
    K --> SP2
    K --> SPN
    SP1 --> DB
    SP2 --> DB
    SPN --> DB
    SP1 --> DLQ
    SP2 --> DLQ
    SPN --> DLQ
    QS --> DB
    UI --> QS
    PS --> PM
    IS --> PM
    SP1 --> PM
    SP2 --> PM
    SPN --> PM
    QS --> PM
    PM --> GF
```

## Components

- `producer-simulator`: emits synthetic telemetry at configurable rates, including duplicates, malformed payloads, and burst traffic
- `ingest-service`: authenticates producers, validates event payloads, records rejections, archives raw events, publishes accepted events to Kafka, and exposes the replay endpoint
- `raw archive`: immutable NDJSON or Blob-backed event log written before broker publish, used for replay and backfills
- `stream-processor`: consumes Kafka partitions, processes partitions in parallel while preserving per-partition ordering, batches hot writes, deduplicates by `event_id`, classifies late events, dead-letters poison records, computes hot and event-time aggregates, and stores per-instance service state snapshots
- `query-service`: exposes tenant-scoped low-latency operational APIs for dashboard reads
- `PostgreSQL`: hot store for aggregate buckets, source counters, rejection history, deduplication state, row-level security, and service state
- `Kafka`: durable broker between write and processing paths, with a dedicated DLQ topic for processor-side poison messages
- `dashboard`: React UI for throughput, lag, latency, and rejection visibility
- `Prometheus` and `Grafana`: metrics collection and local operational visibility
- `asyncapi.yaml` plus JSON Schemas: checked-in asynchronous contract for Kafka topics, headers, and payloads

## Write path

```mermaid
sequenceDiagram
    participant Producer as producer-simulator
    participant Ingest as ingest-service
    participant Archive as raw archive
    participant Kafka as Kafka
    participant Processor as stream-processor
    participant Postgres as PostgreSQL

    Producer->>Ingest: POST /api/v1/events
    Ingest->>Ingest: authenticate and validate schema
    Ingest->>Archive: append raw payload
    Ingest->>Kafka: publish accepted event
    Kafka-->>Processor: deliver message
    Processor->>Processor: decode, batch by partition, classify lateness, and deduplicate
    Processor->>Postgres: commit dedup claims, hot aggregates, and event-time windows
    Processor->>Kafka: commit offsets after DB success
```

Poison-message path:

`Kafka -> stream-processor -> pulsestream.events.dlq`

## Read path

```mermaid
sequenceDiagram
    participant UI as dashboard
    participant Query as query-service
    participant Postgres as PostgreSQL

    UI->>Query: GET /api/v1/metrics/overview
    Query->>Query: authorize tenant scope
    Query->>Postgres: read overview and service state
    Postgres-->>Query: aggregates and recent rejections
    Query-->>UI: overview payload
```

## Replay path

```mermaid
sequenceDiagram
    participant Admin as operator
    participant Ingest as ingest-service
    participant Archive as raw archive
    participant Kafka as Kafka
    participant Processor as stream-processor
    participant Postgres as PostgreSQL

    Admin->>Ingest: POST /api/v1/admin/replay
    Ingest->>Archive: scan archived NDJSON or Blob records
    Ingest->>Kafka: republish matching events
    Kafka-->>Processor: replayed messages
    Processor->>Postgres: reapply aggregates, skip duplicates
```

## Processor scaling model

```mermaid
flowchart TB
    K[(Kafka topic partitions)]
    P1[stream-processor-1]
    P2[stream-processor-2]
    P3[stream-processor-3]
    SS[(service_state)]
    Q[query-service]

    K --> P1
    K --> P2
    K --> P3
    P1 --> SS
    P2 --> SS
    P3 --> SS
    SS --> Q
```

Each processor replica writes an instance-scoped heartbeat and metric snapshot into `service_state`. The query layer only reads recent snapshots, so stopped or replaced replicas do not continue to count as active capacity.

## Design decisions

| Decision | Current choice | Reason |
| --- | --- | --- |
| Broker | Kafka locally, Event Hubs-compatible settings for Azure | Kafka gives strong local reproducibility and direct visibility into partitions, offsets, and consumer groups |
| Hot store | PostgreSQL | One operational store is simpler than a Redis plus PostgreSQL split at this stage |
| Delivery semantics | Idempotent at-least-once | Easier to implement and explain than exactly-once while still demonstrating correctness controls |
| Cold path | Local NDJSON archive plus Blob-backed archive option | Supports replay and rebuild locally while preserving an Azure durability path |
| Local deployment | Docker Compose | Faster iteration and lower operational overhead than Kubernetes for the current stage |
| Replica observability | Prometheus Docker service discovery | Allows local processor scaling evidence without static scrape targets |
| Poison-message handling | Dedicated Kafka DLQ | Keeps processor-side bad records from blocking the consumer loop |
| Contract governance | AsyncAPI plus JSON Schema | Kafka topics and payloads are versioned in source control and validated in CI |
| Stream framework | Custom Go processor, with Flink and Kafka Streams as reference standards | Throughput and semantics should be credible before adding a framework dependency |

## Data owned by PostgreSQL

- `tenant_metrics`: legacy 10-second tenant aggregate buckets kept in rollup reads for compatibility
- `tenant_metric_shards`: current 10-second tenant aggregate shards used to reduce hot-row contention under high load
- `source_metrics`: cumulative source counts for top-N queries
- `event_windows`: fixed 1-minute and 5-minute event-time windows by tenant and source
- `processed_events`: deduplication keys
- `rejection_events`: ingest-side validation and publish failures
- `service_state`: per-instance snapshots for ingest, query, and processor services

## Operational notes

- Request tracing is wired in through OpenTelemetry, but trace export is disabled by default for throughput runs.
- Prometheus scrapes services directly from Docker discovery metadata rather than static target lists.
- Grafana is provisioned only as a local visualization layer. The query API remains the system-of-record read surface for the dashboard.
- Processor snapshot payloads carry `dead_letter_total`, `late_event_total`, active partitions, per-partition ownership, in-flight message count, batch flush metrics, and p50/p95/p99 processing latency.
- New local archive records are partitioned by UTC day, tenant, and hour. Replay still falls back to the legacy date-only layout for older artifacts.

## Azure deployment path

The first Azure deployment variant uses:

- Azure Event Hubs as the Kafka endpoint
- Azure Blob Storage for raw archive durability and replay scanning
- Azure Container Apps for `ingest-service`, `query-service`, and `stream-processor`
- Azure Log Analytics through the Container Apps environment
- system-assigned managed identity on `ingest-service` for Blob access
- existing PostgreSQL and Event Hubs credentials injected as Container Apps secrets

This path provides a durable Azure replay archive. The main remaining Azure gaps are dashboard deployment parity, infrastructure provisioning for backing services, and published Azure benchmark evidence.
