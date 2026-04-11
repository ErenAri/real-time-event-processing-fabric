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

This removes Kafka and PostgreSQL local volumes and resets the hot views and local Prometheus data.

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

## Scale processor replicas

```powershell
docker compose -f deploy/docker-compose/docker-compose.yml up -d --scale stream-processor=3 stream-processor prometheus
```

Prometheus must be included in the command because scrape target discovery depends on the current Docker container set.

## Run a benchmark

```powershell
./scripts/load-test/benchmark.ps1 -Rate 1500 -DurationSeconds 30 -WarmupSeconds 5 -ProcessorReplicas 3
```

The script writes a JSON artifact to `artifacts/benchmarks/`.

## Run a restart drill

```powershell
./scripts/chaos/restart-processor.ps1 -Rate 1000 -DurationSeconds 30 -WarmupSeconds 5 -ProcessorReplicas 3
```

The script writes a JSON artifact to `artifacts/failure-drills/`.

## Replay archived events

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/v1/admin/replay `
  -Headers @{ "X-Admin-Token" = "pulsestream-dev-admin" } `
  -ContentType "application/json" `
  -Body '{"start_date":"2026-04-10","tenant_id":"tenant_01","limit":500}'
```

The archive is stored in the ingest container at `/var/lib/pulsestream/archive` and on the host through the `archive-data` Docker volume.

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

## Investigate rejection spikes

1. Query `GET /api/v1/metrics/rejections`.
2. Query Prometheus for `pulsestream_ingest_rejected_total`.
3. Confirm whether malformed payload injection is expected from the simulator.
4. Inspect ingest logs for `publish_failed` or validation errors.

## Investigate Prometheus target health

1. Open `http://localhost:9090/api/v1/targets?state=any`.
2. Confirm that each processor replica appears as a separate `stream-processor` target.
3. If Docker discovery is empty, recreate Prometheus:

   ```powershell
   docker compose -f deploy/docker-compose/docker-compose.yml up -d --force-recreate prometheus
   ```
