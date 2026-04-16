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

This removes Kafka and PostgreSQL local volumes and resets the hot views.

## Inspect health

- Ingest health: `http://localhost:8080/healthz`
- Query health: `http://localhost:8081/healthz`
- Simulator metrics: `http://localhost:8083/metrics`
- Processor replica status: `docker compose -f deploy/docker-compose/docker-compose.yml ps stream-processor`
- Processor metrics: query Prometheus at `http://localhost:9090`

## Replay archived events

```powershell
Invoke-RestMethod `
  -Method Post `
  -Uri http://localhost:8080/api/v1/admin/replay `
  -Headers @{ "X-Admin-Token" = "pulsestream-dev-admin" } `
  -ContentType "application/json" `
  -Body '{"start_date":"2026-04-10","tenant_id":"tenant_01","limit":500}'
```

The archive is stored in the ingest container at `/var/lib/pulsestream/archive` and on the host via the `archive-data` Docker volume.

Use the scripted replay drill when you need repeatable evidence that replay is duplicate-safe and can rebuild scoped hot views:

```powershell
./scripts/chaos/replay-archive.ps1 -EventCount 25 -WaitTimeoutSeconds 90
```

The drill creates a unique sentinel tenant, verifies the first replay is discarded as duplicates without aggregate overcounting, deletes only the sentinel tenant's hot-view and dedup rows, replays again, and writes an artifact under `artifacts/failure-drills/`.

## Investigate lag

1. Check `pulsestream_processor_consumer_lag` in Prometheus.
2. Confirm the processor replicas are running with `docker compose ... ps stream-processor`.
3. Inspect processor logs for database or decode failures.
4. Compare ingest accepted totals against processed totals in the overview API.

## Investigate rejection spikes

1. Query `GET /api/v1/metrics/rejections`.
2. Inspect `pulsestream_ingest_rejected_total` by `reason`.
3. Confirm whether the simulator is intentionally emitting malformed payloads or if Kafka publishing is failing.

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

## Recovery drills

- Restart processor: `./scripts/chaos/restart-processor.ps1`
- Inject poison message: `./scripts/chaos/inject-poison-message.ps1`
- Pause Postgres: `./scripts/chaos/pause-postgres.ps1`
- Replay archive and rebuild hot views: `./scripts/chaos/replay-archive.ps1`
