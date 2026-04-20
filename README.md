# PulseStream

PulseStream is a real-time event analytics platform for synthetic telemetry. It ingests events through a Go HTTP service, publishes them to Kafka, processes them in near real time, stores hot operational views in PostgreSQL, archives raw payloads for replay, and exposes a React dashboard plus operator APIs.

## What the project demonstrates

- Event-driven architecture with a broker-backed write path
- Idempotent at-least-once processing with duplicate suppression
- Bounded processor batching, event-time windows, late-event counters, and partition/task visibility
- Separation of hot operational state and cold raw event storage
- JWT authentication, tenant-scoped authorization, and PostgreSQL row-level security
- Failure handling with restart, poison-message, broker-outage, Postgres-pause, and replay drills
- Processor replica scaling with measured throughput and lag
- Operational telemetry through Prometheus, Grafana, structured logs, and OpenTelemetry hooks
- AsyncAPI and JSON Schema contract governance
- Azure-aligned runtime path through Event Hubs-compatible Kafka settings, Blob-backed archive support, and Container Apps deployment scaffolding

## Architecture

```mermaid
flowchart LR
    PS[producer-simulator]
    IS[ingest-service]
    RA[(raw archive)]
    K[(Kafka topic)]
    DLQ[(Kafka DLQ)]
    SP1[stream-processor replica 1]
    SPN[stream-processor replica N]
    DB[(PostgreSQL hot views)]
    QS[query-service]
    UI[React dashboard]
    PM[Prometheus]
    GF[Grafana]

    PS -->|HTTP events| IS
    IS -->|append raw payload| RA
    IS -->|publish| K
    K --> SP1
    K --> SPN
    SP1 -->|poison records| DLQ
    SPN -->|poison records| DLQ
    SP1 -->|aggregate and deduplicate| DB
    SPN -->|aggregate and deduplicate| DB
    QS -->|read| DB
    UI -->|poll| QS
    IS --> PM
    SP1 --> PM
    SPN --> PM
    QS --> PM
    PS --> PM
    PM --> GF
```

## Services

| Component | Responsibility |
| --- | --- |
| `producer-simulator` | Generates synthetic telemetry, duplicates, malformed payloads, and burst traffic |
| `ingest-service` | Authenticates producers, validates events, records rejections, writes raw archive entries, and publishes to Kafka |
| `stream-processor` | Consumes Kafka partitions, batches per-partition writes, deduplicates by `event_id`, classifies late events, dead-letters poison records, computes hot and event-time aggregates, and writes service snapshots |
| `query-service` | Serves overview, tenant-series, event-window, partition-health, top-source, rejection, and tenant-scoped dashboard APIs |
| `dashboard` | Renders live operator views, event-time windows, partition health, replay controls, and evidence gates |
| `Prometheus` and `Grafana` | Scrape and display platform metrics |

## Current evidence

| Scenario | Artifact | Summary |
| --- | --- | --- |
| 2k performance gate | `artifacts/benchmarks/benchmark-performance-gate-20260420-160655.json` | `4` producers, `3` processors, target `2,000 eps`; observed `717.1 accepted eps`, `495.08 processed eps`, query `p95 265.82 ms`, drain `39.53s`; target not met |
| 5k offered-load benchmark | `artifacts/benchmarks/benchmark-20260417-222710.json` | `4` producers, `3` processors, target `5,000 eps`; observed `955.91 accepted eps`, `329.37 processed eps`, query `p95 147.13 ms`, peak lag `10,969`; target not met |
| Processor restart drill | `artifacts/failure-drills/restart-processor-20260417-225121.json` | `3` processors, `300 eps`; one replica restarted, lag recovered in `6.29s`, final lag `0` |
| Broker outage drill | `artifacts/failure-drills/broker-outage-20260417-224838.json` | `10s` Kafka outage, archive accounting gap `0`, accepted traffic recovered in `2.09s`, `4,008` explicit publish failures |
| PostgreSQL pause drill | `artifacts/failure-drills/pause-postgres-20260417-224710.json` | `10s` Postgres pause, `3` overview API failures, processor progress resumed `0.02s` after Postgres became healthy |
| Replay and rebuild drill | `artifacts/failure-drills/replay-archive-20260417193652.json` | `25` duplicate replays produced `0` source-metric overcount; scoped hot-view reset rebuilt processed/source counts back to `25` |
| Poison-message drill | `artifacts/failure-drills/inject-poison-message-20260417-193308.json` | malformed Kafka record produced `dead_letter_delta: 1` without blocking the processor loop |

Current local evidence is deliberately conservative. Recovery behavior is verified at sustainable rates, but the intermediate `2,000 processed eps` gate and MVP `5,000 eps` throughput target are not yet met on this machine. The current bottleneck is the local write path under high offered load: producer/client timeouts, ingest publish/archive pressure, and PostgreSQL hot-view writes limit accepted and processed throughput. The latest 2k gate also pushed overview query p95 above the `250 ms` target.

## Quick start

1. Start the local stack.

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml up --build
   ```

2. Open the local surfaces.

   - Dashboard: `http://localhost:4173`
   - Query API: `http://localhost:8081/api/v1/metrics/overview` with `Authorization: Bearer <jwt>`
   - Ingest API: `http://localhost:8080/api/v1/events`
   - Prometheus: `http://localhost:9090`
   - Grafana: `http://localhost:3000` with `admin` / `admin`

3. Run the current performance gate.

   ```powershell
   ./scripts/load-test/run-performance-gate.ps1 -Rate 2000 -DurationSeconds 60 -WarmupSeconds 10 -ProcessorReplicas 3 -ProducerCount 4 -MaxInFlight 768 -TenantCount 50 -SourcesPerTenant 200
   ```

4. Run a custom benchmark.

   ```powershell
   ./scripts/load-test/benchmark.ps1 -Rate 1500 -DurationSeconds 30 -WarmupSeconds 5 -ProcessorReplicas 3
   ```

5. Run the current 5k offered-load benchmark profile.

   ```powershell
   ./scripts/load-test/benchmark.ps1 -Rate 5000 -ProducerCount 4 -DurationSeconds 60 -WarmupSeconds 10 -ProcessorReplicas 3 -MaxInFlight 1024 -TenantCount 50 -SourcesPerTenant 200
   ```

6. Run a restart drill.

   ```powershell
   ./scripts/chaos/restart-processor.ps1 -Rate 300 -DurationSeconds 45 -WarmupSeconds 5 -ProcessorReplicas 3
   ```

7. Run the replay and rebuild drill.

   ```powershell
   ./scripts/chaos/replay-archive.ps1 -EventCount 25 -WaitTimeoutSeconds 90
   ```

8. Validate the asynchronous contract and evidence summary.

   ```powershell
   npm install
   npm run contract:validate
   npm run evidence:validate
   ```

## Local auth

JWT auth and tenant-scoped authorization are enabled in the local stack. The dashboard and simulator images are built with a development admin token so the default operator path works after `docker compose up`.

For manual API access, mint a token with the local development secret:

```powershell
go run ./cmd/dev-token `
  -role admin `
  -subject local-admin `
  -secret pulsestream-dev-secret
```

Admin tokens can query any tenant and call replay endpoints. `tenant_user` tokens are restricted to their assigned `tenant_id`. Health, readiness, and Prometheus metrics endpoints remain unauthenticated.

## Repository layout

```text
services/
  producer-simulator/
  ingest-service/
  stream-processor/
  query-service/
internal/
  api/
  archive/
  events/
  platform/
  processor/
  simulator/
  store/
  telemetry/
web/dashboard/
deploy/docker-compose/
deploy/azure/container-apps/
docs/
scripts/
schemas/
asyncapi.yaml
```

## Documentation

- [Architecture](docs/architecture.md)
- [API specification](docs/api-spec.md)
- [Data model](docs/data-model.md)
- [Processing guarantees](docs/processing-guarantees.md)
- [Benchmarking](docs/benchmarking.md)
- [Failure modes](docs/failure-modes.md)
- [Runbook](docs/runbook.md)

## Contract governance

- [asyncapi.yaml](asyncapi.yaml) documents the Kafka topics, operations, headers, and examples for `pulsestream.events` and `pulsestream.events.dlq`
- [telemetry-event-v1.schema.json](schemas/telemetry-event-v1.schema.json) is the source payload schema for accepted telemetry events
- [dead-letter-record-v1.schema.json](schemas/dead-letter-record-v1.schema.json) defines the processor-side poison-message payload
- GitHub Actions validates the AsyncAPI document on every push and pull request

## Azure variant

- The Kafka client layer supports local `PLAINTEXT` Kafka and Azure Event Hubs via `SASL_SSL` plus `PLAIN` credentials from environment variables
- Azure Container Apps deployment scaffolding for the backend services lives under [deploy/azure/container-apps](deploy/azure/container-apps)
- The ingest service supports a Blob-backed raw archive for durable Azure replay, using managed identity by default
- The deployment template assumes existing Event Hubs, Blob Storage, and PostgreSQL dependencies and focuses on application hosting first

## Current limits

- Redis caching is not part of the current hot path; PostgreSQL remains the only operational read store.
- Azure dashboard deployment and published Azure benchmark evidence are still follow-on work.
- The intermediate `2,000 processed eps` gate is not met yet. The latest local gate accepted `717.1 eps` and processed `495.08 eps`; slow stages were ingest archive write, Kafka publish, and PostgreSQL tenant/window aggregate upserts.
- New archive records use tenant/hour prefixes for scoped replay. Existing date-only archives are still readable, and old replay evidence remains useful as the baseline for why indexing was added.
- Controlled recovery drills pass at sustainable rates; overload drills are still useful, but should be documented as degraded-mode evidence rather than success evidence.
