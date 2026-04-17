# Runbook

## Start the stack

```powershell
docker compose -f deploy/docker-compose/docker-compose.yml up --build
```

## Stop the stack

```powershell
docker compose -f deploy/docker-compose/docker-compose.yml down
```

## Reset local state

```powershell
docker compose -f deploy/docker-compose/docker-compose.yml down -v
```

This removes Kafka, PostgreSQL, and local Prometheus data and resets the hot views.

## Local endpoints

| Surface | URL |
| --- | --- |
| Dashboard | `http://localhost:4173` |
| Ingest API | `http://localhost:8080` |
| Query API | `http://localhost:8081` |
| Prometheus | `http://localhost:9090` |
| Grafana | `http://localhost:3000` |

## Health checks

| Service | Check |
| --- | --- |
| `ingest-service` | `http://localhost:8080/healthz` |
| `query-service` | `http://localhost:8081/healthz` |
| `producer-simulator` | `http://localhost:8083/healthz` |
| `stream-processor` replicas | `docker compose -f deploy/docker-compose/docker-compose.yml ps stream-processor` |

## Mint a local token

```powershell
go run ./cmd/dev-token `
  -role admin `
  -subject local-admin `
  -secret pulsestream-dev-secret
```

Use the resulting value as `Authorization: Bearer <jwt>` for ingest and query APIs.

## Scale processor replicas

```powershell
docker compose -f deploy/docker-compose/docker-compose.yml up -d --no-deps --scale stream-processor=3 stream-processor
```

Prometheus uses Docker service discovery, so it should discover the new processor containers without being recreated. Avoid including `prometheus` in this command during drills because its compose dependencies can restart the steady-state `producer-simulator` and contaminate load evidence.

## Run a benchmark

```powershell
./scripts/load-test/benchmark.ps1 -Rate 1500 -DurationSeconds 30 -WarmupSeconds 5 -ProcessorReplicas 3
```

The script writes a JSON artifact to `artifacts/benchmarks/` and refreshes `artifacts/evidence/latest.json`.

Run the current 5k offered-load profile when you need to reproduce the known throughput gap:

```powershell
./scripts/load-test/benchmark.ps1 -Rate 5000 -ProducerCount 4 -DurationSeconds 60 -WarmupSeconds 10 -ProcessorReplicas 3 -MaxInFlight 1024 -TenantCount 50 -SourcesPerTenant 200
```

The latest run of that profile accepted `955.91 eps` and processed `329.37 eps`, so it is a bottleneck artifact rather than a passing MVP result.

## Run a restart drill

```powershell
./scripts/chaos/restart-processor.ps1 -Rate 300 -DurationSeconds 45 -WarmupSeconds 5 -ProcessorReplicas 3
```

The script writes a JSON artifact to `artifacts/failure-drills/` and refreshes `artifacts/evidence/latest.json`.

Use higher rates to test degraded behavior separately. A recent `800 eps` restart run continued processing but did not fully recover lag inside the observation window.

## Refresh operator evidence

If you add or copy artifacts manually, regenerate the dashboard evidence summary:

```powershell
./scripts/evidence/update-evidence.ps1
```

Validate the schema:

```powershell
npm run evidence:validate
```

Read the summary through the API:

```powershell
Invoke-RestMethod `
  -Uri http://localhost:8081/api/v1/evidence/latest `
  -Headers @{ Authorization = "Bearer <admin-jwt>" }
```

The dashboard reads this endpoint for the latest benchmark and failure-drill cards.

## Replay archived events

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/v1/admin/replay `
  -Headers @{ "X-Admin-Token" = "pulsestream-dev-admin" } `
  -ContentType "application/json" `
  -Body '{"start_date":"2026-04-10","tenant_id":"tenant_01","limit":500}'
```

The archive is stored in the ingest container at `/var/lib/pulsestream/archive` and on the host through the `archive-data` Docker volume. New records use `year/month/day/tenant_id/hour/events.ndjson`; replay still falls back to the legacy `year/month/day/events.ndjson` layout for older artifacts.

Use the scripted replay drill when you need repeatable evidence that replay is duplicate-safe and can rebuild scoped hot views:

```powershell
./scripts/chaos/replay-archive.ps1 -EventCount 25 -WaitTimeoutSeconds 90
```

The drill creates a unique sentinel tenant, verifies the first replay is discarded as duplicates without aggregate overcounting, deletes only the sentinel tenant's hot-view and dedup rows, replays again, and writes an artifact under `artifacts/failure-drills/`.

## Investigate lag

1. Query Prometheus for `sum(pulsestream_processor_consumer_lag)`.
2. Check processor replicas with:

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml ps stream-processor
   ```

3. Inspect processor logs:

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml logs stream-processor
   ```

4. Compare `accepted_total` and `processed_total` in `GET /api/v1/metrics/overview`.
5. Query `GET /api/v1/metrics/partitions` to identify hot partitions, owner processor instances, and partition-level in-flight work.

## Investigate rejection spikes

1. Query `GET /api/v1/metrics/rejections`.
2. Query Prometheus for `pulsestream_ingest_rejected_total`.
3. Confirm whether malformed payload injection is expected from the simulator.
4. Inspect ingest logs for `publish_failed`, `backpressure`, or validation errors.

## Investigate dead-letter activity

1. Query `GET /api/v1/metrics/overview` and check `dead_letter_total`.
2. Inspect processor logs for `message_dead_lettered`.
3. Run the scripted poison-message drill when you need a clean end-to-end verification path:

   ```powershell
   ./scripts/chaos/inject-poison-message.ps1
   ```

4. Read the DLQ topic from Kafka:

   ```powershell
   docker exec docker-compose-kafka-1 sh -lc `
     "/opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic pulsestream.events.dlq --from-beginning --max-messages 10 --timeout-ms 5000"
   ```

5. Confirm the DLQ record contains the expected source topic, source offset, consumer group, and reason before deciding whether to replay or discard the source data.

## Investigate Prometheus target health

1. Open `http://localhost:9090/api/v1/targets?state=any`.
2. Confirm that each processor replica appears as a separate `stream-processor` target.
3. If Docker discovery is empty, recreate Prometheus:

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml up -d --force-recreate prometheus
   ```

## Recovery drills

- Restart processor: `./scripts/chaos/restart-processor.ps1`
- Inject poison message: `./scripts/chaos/inject-poison-message.ps1`
- Pause Postgres: `./scripts/chaos/pause-postgres.ps1`
- Broker outage: `./scripts/chaos/broker-outage.ps1`
- Replay archive and rebuild hot views: `./scripts/chaos/replay-archive.ps1`

Current controlled drill commands:

```powershell
./scripts/chaos/restart-processor.ps1 -Rate 300 -DurationSeconds 45 -WarmupSeconds 5 -ProcessorReplicas 3
./scripts/chaos/pause-postgres.ps1 -Rate 500 -DurationSeconds 45 -WarmupSeconds 5 -PauseSeconds 10 -ProcessorReplicas 3
./scripts/chaos/broker-outage.ps1 -Rate 400 -DurationSeconds 50 -WarmupSeconds 5 -OutageSeconds 10 -ProcessorReplicas 3
./scripts/chaos/inject-poison-message.ps1 -TimeoutSeconds 60 -ReadyTimeoutSeconds 20
./scripts/chaos/replay-archive.ps1 -EventCount 25 -WaitTimeoutSeconds 120
```
