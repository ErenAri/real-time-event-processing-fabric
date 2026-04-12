# Failure Modes

## Processor restart during load

- Trigger: `./scripts/chaos/restart-processor.ps1`
- Scale-aware option: `./scripts/chaos/restart-processor.ps1 -ProcessorReplicas 3`
- Expected behavior: ingest continues accepting traffic, Kafka buffers messages, processor lag spikes and then drains after restart
- Observed run: `artifacts/failure-drills/restart-processor-20260410-192413.json`
- Observed behavior: at `1000 eps` target load, ingest continued accepting traffic with `0` new rejections while the processor container restarted in roughly `1.54s`
- Observed behavior: accepted traffic increased by `22,997` events during the drill, processed traffic increased by `7,571`, duplicate discards increased by `13`
- Observed behavior: consumer lag was already elevated before restart (`60,351`), peaked at `94,342`, and did not recover within the `30s` observation window; final lag remained `77,065`
- Observed rerun after processor optimization: `artifacts/failure-drills/restart-processor-20260410-194815.json`
- Observed rerun behavior: accepted traffic increased by `22,977` events, processed traffic increased by `19,479`, duplicate discards increased by `104`, and `0` new rejections were recorded
- Observed rerun behavior: the processor still did not recover within the `30s` window because pre-existing lag was already high (`143,169` before restart), but the processor did materially more useful work during the drill and kept `p95`/`p99` processing latency at `13ms` / `25ms`
- Observed multi-replica rerun: `artifacts/failure-drills/restart-processor-20260410-212812.json`
- Observed multi-replica behavior: with `3` processor replicas and a `1000 eps` target load, restarting one replica increased accepted traffic by `27,406` events while processed traffic increased by `31,860`; no new rejections were recorded and `p95`/`p99` stayed at `11ms` / `19ms`
- Observed multi-replica behavior: consumer lag started at `0`, peaked at `828`, and remained `828` at the end of the `30s` drill window, so the group absorbed the restart cleanly but still did not fully drain the backlog inside the observation window
- Interpretation: restart recovery mechanics work and the optimized processor is materially stronger than the first run, but the system still needs more sustained processor capacity before claiming near-real-time catch-up under continuous `1000 eps` load with existing backlog
- Current drill tooling restarts a single processor replica container and samples aggregate processor counters through Prometheus, so the same script can now be used for single-replica or scaled consumer-group recovery evidence
- Recovery lever: use the replay endpoint if hot views must be rebuilt from the raw archive

## Duplicate event injection

- Trigger: simulator configuration with `SIM_DUPLICATE_EVERY`
- Expected behavior: ingest accepts duplicates, processor discards them via `processed_events`
- Evidence to capture: `duplicate_total` increases while tenant aggregates do not overcount

## Malformed payload burst

- Trigger: simulator configuration with `SIM_MALFORMED_EVERY`
- Expected behavior: ingest returns `400`, records rejection rows, and continues serving valid traffic
- Evidence to capture: rejection timeline and `pulsestream_ingest_rejected_total`

## Poison message already in Kafka

- Trigger: `./scripts/chaos/inject-poison-message.ps1` or write a malformed or semantically invalid record directly to Kafka
- Expected behavior: processor publishes one DLQ record, commits the source offset only after the DLQ write succeeds, and increments `dead_letter_total`
- Observed drill: `artifacts/failure-drills/inject-poison-message-20260411-152328.json`
- Observed behavior: the scripted drill paused the compose simulator, launched a temporary processor with a fresh consumer group at the current topic tail, wrote one malformed record directly to `pulsestream.events`, and moved the overview API from `dead_letter_total: 0` to `dead_letter_total: 1`
- Observed behavior: the DLQ record captured the failure reason, source topic, source offset, consumer group, and base64-encoded original payload
- Interpretation: processor-side poison messages are isolated without blocking the consumer loop, and the operator path can see the event through both the overview API and the DLQ topic even when the main consumer group is already carrying backlog

## PostgreSQL pause or slowdown

- Trigger: `./scripts/chaos/pause-postgres.ps1`
- Expected behavior: processor errors become visible quickly, read paths degrade, recovery begins when Postgres resumes
- Evidence to capture: service logs, dashboard state, and recovery time after unpause

## Broker outage

- Trigger: `./scripts/chaos/broker-outage.ps1`
- Expected behavior: ingest publish failures become visible quickly, accepted traffic drops during the outage window, the raw archive still retains valid requests, and the processor resumes consumption after broker health returns without crashing the service
- Observed drill before processor retry hardening: `artifacts/failure-drills/broker-outage-20260412-195145.json`
- Observed behavior before hardening: with an `800 eps` target load and a `12s` Kafka outage, the archive delta was `11,720`, accepted traffic increased by `8,722`, explicit `publish_failed` rejections increased by `2,000`, and the accounting gap remained `998`
- Observed behavior before hardening: the producer log showed large numbers of HTTP client timeouts and the processor log showed a fatal `fetch kafka message` panic when the broker connection was refused
- Observed drill after processor retry hardening: `artifacts/failure-drills/broker-outage-20260412-200340.json`
- Observed behavior after hardening: the archive delta increased by `26,160`, accepted traffic increased by `20,391`, explicit `publish_failed` rejections increased by `5,767`, and the accounting gap dropped to `2`
- Observed behavior after hardening: the processor stayed live and emitted `fetch_message_failed_retrying` warnings instead of exiting, accepted traffic recovered within the drill window, and `p95` / `p99` processing latency peaked at `47ms` / `143ms`
- Interpretation: broker outage handling is now materially stronger because the consumer no longer crashes on broker loss and the ingest path surfaces nearly all failed publishes explicitly; the remaining gap is tightening overload behavior at the ingest edge so client timeouts disappear under sustained broker loss
- Current code now includes an explicit ingest in-flight backpressure gate, but the first rerun after that change hit a timeout in the drill's Kafka-health wait and did not produce a publishable artifact

## Replay and rebuild

- Trigger: `POST /api/v1/admin/replay`
- Expected behavior: archived events are republished to Kafka, duplicates are safely ignored by the processor, and hot views can be rebuilt
- Evidence to capture: replay counts, duplicate counts, and post-replay aggregate correctness
