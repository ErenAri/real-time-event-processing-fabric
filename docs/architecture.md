# Architecture

## Scope

PulseStream is a local-first event analytics platform that emphasizes distributed-systems concerns over product breadth. The current implementation focuses on ingestion, streaming aggregation, replay, observability, and measurable failure behavior.

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
    Ingest->>Ingest: validate schema and required fields
    Ingest->>Archive: append NDJSON record
    Ingest->>Kafka: publish accepted event
    Kafka-->>Processor: deliver message
    Processor->>Processor: decode and deduplicate
    Processor->>Postgres: upsert hot aggregates and service state
```

## Read path

```mermaid
sequenceDiagram
    participant UI as dashboard
    participant Query as query-service
    participant Postgres as PostgreSQL

    UI->>Query: GET /api/v1/metrics/overview
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
    Ingest->>Archive: scan archived NDJSON files
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
| Broker | Kafka | Strong local reproducibility and direct visibility into partitions, offsets, and consumer groups |
| Hot store | PostgreSQL | One operational store is simpler than a Redis plus PostgreSQL split at this stage |
| Delivery semantics | Idempotent at-least-once | Easier to implement and explain than exactly-once while still demonstrating correctness controls |
| Cold path | Local NDJSON archive | Supports replay and rebuild without requiring cloud object storage |
| Local deployment | Docker Compose | Faster iteration and lower operational overhead than Kubernetes for the current stage |
| Replica observability | Prometheus Docker service discovery | Allows local processor scaling evidence without static scrape targets |

## Data owned by PostgreSQL

- `tenant_metrics`: 10-second aggregate buckets for tenant charts
- `source_metrics`: cumulative source counts for top-N queries
- `processed_events`: deduplication keys
- `rejection_events`: ingest-side validation and publish failures
- `service_state`: per-instance snapshots for ingest, query, and processor services

## Operational notes

- Request tracing is wired in through OpenTelemetry, but trace export is disabled by default for throughput runs.
- Prometheus scrapes services directly from Docker discovery metadata rather than static target lists.
- Grafana is provisioned only as a local visualization layer. The query API remains the system-of-record read surface for the dashboard.
