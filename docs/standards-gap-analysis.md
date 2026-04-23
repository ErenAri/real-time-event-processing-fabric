# Standards Gap Analysis

## Purpose

This document compares PulseStream with the practical standards set by Apache Flink and Kafka Streams. The goal is not to claim framework parity. The goal is to identify what is already credible, what is still missing, and what should be improved next for a senior-level portfolio assessment.

Primary references:

- [Apache Flink stateful stream processing](https://nightlies.apache.org/flink/flink-docs-release-1.18/docs/concepts/stateful-stream-processing/)
- [Apache Flink watermarks](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/event-time/generating_watermarks/)
- [Kafka Streams architecture](https://kafka.apache.org/26/streams/architecture/)
- [Kafka Streams core concepts](https://kafka.apache.org/20/streams/core-concepts/)
- [Kafka Streams streams and tables](https://kafka.apache.org/40/streams/core-concepts/)

## Current Evidence Position

PulseStream has credible custom streaming-system evidence in these areas:

- Kafka-backed ingestion and consumer processing
- idempotent at-least-once processing with offset commit after DB/DLQ success
- bounded processor batching
- event-time windows with allowed lateness
- duplicate suppression by `event_id`
- raw archive replay with tenant/hour prefixes for new data
- batch ingest for production-shaped producer traffic
- Prometheus metrics, structured logs, dashboard evidence, and failure-drill artifacts
- tenant-scoped auth and PostgreSQL row-level security

The throughput evidence now clears the intermediate and MVP local targets once:

- Latest sharded 5k run: `4,971.03 accepted eps`, `5,101.71 processed eps`, query p95 `54.91 ms`, drain `2.05s`
- Latest batch 5k run: `4,980.08 accepted eps`, `4,962.23 processed eps`, query p95 `52.19 ms`, drain `3.59s`
- Latest batch 2k gate: `1,973.53 accepted eps`, `2,030.64 processed eps`, query p95 `26.24 ms`, drain `0.01s`
- Previous one-event 2k gate: `898.67 accepted eps`, `910.44 processed eps`, query p95 `135.03 ms`, drain `2.02s`
- Intermediate target met: `2,000 processed eps`
- MVP target met once locally: `5,000 processed eps`

## Standards Baseline

Flink standard:

- keyed state is partitioned into key groups for redistribution
- checkpoints combine stream positions and operator state for recovery
- recovery restores state and replays input from checkpointed positions
- event-time processing uses watermarks, idleness detection, and watermark alignment
- production state is stored in a configured durable state backend

Kafka Streams standard:

- input topic partitions define stream tasks and maximum parallelism
- tasks are assigned across stream threads and application instances
- local state stores are fault-tolerant through changelog topics
- streams and tables are first-class abstractions for stateful aggregation
- window grace periods handle out-of-order records
- exactly-once mode can atomically coordinate input offsets, state-store updates, and output topic writes when Kafka is the state/output boundary

## Full Gap List

| Area | PulseStream now | Standard expectation | Gap | Priority |
| --- | --- | --- | --- | --- |
| Sustained throughput | Latest sharded batch result is `5,101.71 processed eps` against a `5,000 eps` gate | Clear sustained throughput with repeatable pass/fail gates | 5k is met once locally; repeatability is not yet proven | P0 |
| Benchmark repeatability | One sharded 5k pass exists after earlier 2026-04-23 runs varied from `910` to `4,962 processed eps` | Low-variance benchmark profile with pinned resources and clean start state | The 5k pass needs repeat runs on clean state before it becomes a headline capacity claim | P0 |
| Ingest publish path | Batch ingest endpoint is implemented and benchmarked at `5,101.71 processed eps` | High-throughput producers usually batch records and tune broker/client I/O | Ingest scale-out and cloud benchmark evidence remain unproven | P1 |
| Ingest scale-out | One exposed ingest-service instance in Compose | Stateless ingest should scale horizontally behind a load balancer | Compose profile cannot scale ingest because host port binding is tied to one container | P0 |
| Producer realism | Simulator supports `SIM_BATCH_SIZE`; latest gate used `4` producers and batch size `25` | Production producers often use batch APIs, persistent clients, and controlled payload distributions | Payload distributions and client connection behavior are still synthetic | P1 |
| Tenant aggregate hot spot | Tenant metrics now write to `tenant_metric_shards`; latest 5k tenant aggregate p95 is `141.7 ms`, down from `558.48 ms` | Stateful processors avoid central DB hot-row contention with local state/changelog or staged merges | Sharding reduces contention, but state is still centralized in PostgreSQL | P1 |
| State backend | PostgreSQL stores dedup, hot aggregates, windows, and service state | Flink/Kafka Streams use keyed/local state plus checkpoints/changelogs | Durable state is simple and inspectable, but DB pressure limits throughput | P0 |
| Checkpointing | Offsets commit after DB/DLQ success; no distributed checkpoints | Flink checkpoints snapshot operator state and input positions consistently | No checkpoint coordinator, barriers, or savepoints | P1 |
| Exactly-once | Idempotent at-least-once with dedup | Kafka Streams exactly-once can atomically coordinate Kafka offsets, state stores, and Kafka outputs | No cross-system transaction between Kafka and PostgreSQL | P1 |
| Watermarks | Per-partition max event time with fixed allowed lateness | Flink watermarks handle event-time progress, idle inputs, and alignment | No distributed watermark propagation, idleness detection, or alignment | P1 |
| Window lifecycle | 1m and 5m event-time windows are stored | Frameworks manage window grace, retention, cleanup, and update semantics | No automated window retention policy or compaction strategy documented for large history | P1 |
| Task model | Processor exposes partition state snapshots and lag | Kafka Streams exposes tasks tied to input partitions and stream threads | Task identity is coarser than Kafka Streams tasks; assignment history is not fully queryable | P1 |
| Partition scaling | Processor replicas can consume partitions | Kafka Streams maximum parallelism is partition-bound and explicit | Partition-count, replica-count, and throughput guidance are not yet documented as a capacity model | P1 |
| Rebalancing | Processor snapshots show assignments after the fact | Frameworks handle assignment, standby state, and recovery internally | Rebalance duration and assignment churn are not benchmarked | P1 |
| Local state recovery | Processor recovery relies on Kafka redelivery plus PostgreSQL idempotency | Kafka Streams can restore local state from changelog topics; Flink restores operator state from checkpoints | No local state restore benchmark because state is centralized in PostgreSQL | P1 |
| Backpressure model | In-flight limits, Kafka publish latency, and rejection metrics exist | Production stream systems expose explicit queue, mailbox, watermark, and sink backpressure | Backpressure is observable but not yet modeled end-to-end in docs or dashboard | P1 |
| Archive durability | Compose uses async archive queue for throughput | Cold storage writes should have explicit durability guarantees and retry behavior | Async archive durability is decoupled from HTTP response; queue overflow policy must be tested under sustained overload | P1 |
| Replay efficiency | New archives use date/tenant/hour layout; legacy fallback exists | Production replay should have selective indexes, progress, cancellation, and rate limits | Replay still scans files within selected hour prefixes and lacks resumable job state | P1 |
| Failure drills after new fixes | Existing drills cover restart, broker outage, Postgres pause, replay, poison message | Failure evidence should be rerun after major hot-path changes | The 2k batch gate now passes, so post-batch failure drills should be rerun | P1 |
| Chaos coverage | Scripts exist for key scenarios | Production systems test dependency slowness, network partitions, broker ISR loss, disk pressure, and rolling deploys | Current drills do not cover broker disk pressure, partition loss, slow Kafka acks, or rolling deployment | P2 |
| Observability depth | Prometheus metrics, structured logs, traces hooks, evidence API | Production standard includes trace correlation through ingest, Kafka, processor, DB, and dashboard queries | Trace evidence is not yet captured as a published artifact | P2 |
| Alerting | Alerts exist in Prometheus config | Operational alerts should be tied to SLOs and runbook actions | Alert thresholds are not yet validated against benchmark and chaos data | P2 |
| Dashboard evidence | Dashboard shows evidence gates and operator views | Production dashboards should separate current health, SLO burn, bottlenecks, and drill history | Dashboard is useful but not yet a complete incident console | P2 |
| Security | JWT roles, tenant filters, RLS | Production multi-tenancy needs key rotation, audit logs, least-privilege infra, and quota enforcement | Auth is credible for portfolio scope but not enterprise-complete | P2 |
| Quotas | Ingest in-flight limit and rate/backpressure signals exist | Per-tenant quotas should be enforced and visible | Per-tenant quota/rate-limit enforcement is not fully proven under benchmark | P2 |
| Schema evolution | Schema version exists; AsyncAPI and JSON Schema are validated | Production event contracts need compatibility checks and migration policy | No compatibility matrix or schema evolution test suite | P2 |
| Deployment | Docker Compose and Azure Container Apps scaffold exist | Production standard includes Kubernetes/Container Apps autoscaling evidence and IaC-managed dependencies | No cloud throughput evidence, no autoscaling evidence, no cost/performance comparison | P2 |
| CI | Tests and contract/evidence validation pass locally | CI should run unit, contract, dashboard build, and selected integration checks | Local validation is strong; CI still should publish artifacts and gate PRs on evidence schemas | P2 |
| Data retention | Archive and hot tables exist | Production systems document retention, compaction, cleanup, and rebuild windows | Retention is not fully enforced across raw archive, dedup table, and window tables | P2 |
| Capacity planning | Benchmark docs include hardware and observed bottlenecks | Senior-level system docs should map partitions, replicas, DB capacity, and throughput | Capacity model is still empirical, not predictive | P2 |
| Cost model | Azure scaffold exists | Cloud-aligned project should include cost/performance tradeoff | No Event Hubs vs Kafka or Postgres sizing benchmark yet | P3 |
| Framework comparison implementation | Flink/Kafka Streams are reference standards only | A comparison processor could demonstrate framework literacy | No alternate Flink or Kafka Streams implementation exists; this is intentionally deferred | P3 |

## What Improved In This Pass

- Raw archive no longer dominates the synchronous ingest request path in the Compose benchmark profile.
- File archive writes no longer reopen the active tenant/hour file for every mixed-tenant event.
- Processor aggregate writes use set-based upserts instead of one statement per aggregate key.
- Processor window/source stage latency improved materially compared with the pre-fix 2k gate.
- Query p95 and post-load drain pass in the latest sharded 5k batch evidence.
- Optional Kafka publish batching was tested and kept disabled because it regressed local evidence.
- Batch ingest plus sharded tenant metrics raised the latest local benchmark to `5,101.71 processed eps` with batch size `25`.

## Recommended Next Work

1. Repeat the 5k sharded batch benchmark on clean state at least three times and record variance.
2. Add an ingest load balancer service in Compose so `ingest-service` can scale horizontally without host port conflicts.
3. Rerun overload-level failure drills against the batch/sharded hot path.
4. Add a clean benchmark profile that can isolate project names or reset volumes for repeatable evidence.
5. Profile Kafka publish p99 and dedup claim p99 under the passing 5k profile.
6. Add a capacity model that ties topic partitions, processor replicas, DB write latency, and expected EPS together.
7. Add retention jobs for `processed_events`, `tenant_metrics`, `source_metrics`, and `event_windows`.
8. Publish trace evidence for one accepted event through ingest, Kafka, processor, DB write, and dashboard query.
9. Add cloud benchmark evidence after the local 5k gate is repeatable.

## Assessment

PulseStream is above a student demo because it has real Kafka ingestion, idempotent processing, event-time windows, replay, failure drills, tenant isolation, and evidence artifacts. It now clears the local 5k processed-eps gate once, but it is not yet at the strongest senior-level performance bar because repeatability, post-change failure evidence, cloud evidence, and framework-grade keyed state/checkpoint semantics remain open.

The strongest honest positioning is:

> Built a custom Go/Kafka real-time analytics platform with idempotent at-least-once processing, event-time windows, tenant-scoped APIs, observability, replay, and failure-drill evidence; current local sharded-batch benchmark reaches `5,101 processed eps` against a `5,000 eps` target, with documented repeatability, post-change failure-drill, and state-model gaps.
