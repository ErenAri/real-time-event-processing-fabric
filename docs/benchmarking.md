# Benchmarking

## Goal

The MVP benchmark gate is `5k events/sec` sustained with live dashboard freshness under 2 seconds and no correctness regression in duplicate handling or poison-message containment.

## Local benchmark procedure

1. Start the Compose stack:

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml up --build
   ```

2. Run the benchmark driver:

   ```powershell
   ./scripts/load-test/benchmark.ps1 -Rate 1500 -DurationSeconds 60 -ProcessorReplicas 3
   ```

3. The benchmark driver now defaults to `-ProducerMode compose`, which runs a one-off `producer-simulator` inside the Docker network for a more representative producer path than a Windows host process.

4. The benchmark driver now:

   - stops the steady-state compose `producer-simulator` so the run is isolated
   - optionally scales `stream-processor` to the requested replica count before the run
   - starts a one-off benchmark producer container with explicit load-profile overrides
   - samples `GET /api/v1/metrics/overview` through the full run
   - sources throughput counters from Prometheus instant queries so multi-replica processor totals remain correct
   - emits a JSON report under `artifacts/benchmarks/`

5. Capture evidence from:

   - `GET /api/v1/metrics/overview`
   - Prometheus queries for ingest and processor counters
   - dashboard screenshots showing lag and rejection trends

## Metrics to record

- accepted events per second
- processed events per second
- consumer lag
- processing latency p50, p95, p99
- rejection rate
- duplicate discard count
- dead-letter count
- observed processor replica count
- summed active partitions and in-flight backlog across processor replicas

## Evidence table

Fill this table after each benchmark run.

| Date | Rate target | Duration | Accepted eps | Processed eps | P95 ms | P99 ms | Lag peak | Notes |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 2026-04-10 | 1500 | 14s | 1209.48 | 330.52 | 18 | 28 | 52656 | compose producer baseline before processor optimization, artifact `artifacts/benchmarks/benchmark-20260410-191942.json` |
| 2026-04-10 | 1500 | 14s | 1219.12 | 875.72 | 21 | 38 | 198421 | compose producer after partition-parallel processor workers and single-statement hot-store write, artifact `artifacts/benchmarks/benchmark-20260410-194727.json` |
| 2026-04-10 | 1500 | 33s | 713.09 | 568.43 | 14 | 23 | 1308 | compose producer, exact-count harness, `1` processor replica, artifact `artifacts/benchmarks/benchmark-20260410-212955.json` |
| 2026-04-10 | 1500 | 33s | 700.37 | 595.02 | 11 | 19 | 1246 | compose producer, exact-count harness, `3` processor replicas, artifact `artifacts/benchmarks/benchmark-20260410-213110.json` |

## Current readout

- The benchmark harness is now producing credible artifacts for the current local stack.
- The ingest path is sustaining roughly `1.2k accepted eps` under the captured Compose-network benchmark profile.
- Processor throughput improved from roughly `331 processed eps` to roughly `876 processed eps` after introducing partition-parallel workers and collapsing the Postgres write path into one atomic statement.
- The stream processor is still the current bottleneck: even after the optimization pass, consumer lag continued to grow under the `1500 eps` target load.
- The scale-aware benchmark harness now compares `1` vs `3` processor replicas under the same script behavior. In the latest pair of runs, `3` replicas improved processed throughput from `568.43 eps` to `595.02 eps`, while also improving `p95`/`p99` latency from `14/23 ms` to `11/19 ms`.
- The latest pair also shows the current ceiling is no longer purely processor-side in this profile: the offered producer rate settled around `700-860 eps`, so the next benchmark pass should raise producer capacity or parallelism if the goal is to isolate processor scaling more aggressively.
- On Windows, a host-native benchmark producer materially under-reported throughput compared with the Compose-network producer path, so `-ProducerMode compose` is the baseline method for published evidence.
- Poison-message handling is verified separately from the throughput harness in `artifacts/failure-drills/dead-letter-verification-20260411-1509.json` so malformed direct-to-Kafka records can be tested without distorting the hot benchmark stream.

## Next bottleneck

The next performance gate is still processor throughput and lag recovery under sustained load. The next credible path is either increasing effective consumer parallelism further, reducing hot-store write cost again, or adding horizontal processor replicas with consumer-group evidence. Any claim above the current benchmark row should be backed by new artifacts after one of those changes.

## Scale-aware note

The platform now supports scale-aware processor measurements:

- `service_state` is aggregated by live `instance_id`
- Prometheus discovers processor replicas from Docker rather than a single static target
- benchmark artifacts record requested and observed processor replica counts

Fresh multi-replica benchmark artifacts should be captured after the local Docker environment is stable again.
