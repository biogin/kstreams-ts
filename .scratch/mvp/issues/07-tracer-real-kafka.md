# 07: Tracer bullet on real Kafka

**What to build:** The pass-through app from ticket 02 runs against a real broker with `workers: 0` and at-least-once: it consumes, runs the graph, produces, waits for acks, then commits offsets. `start`/`stop` and basic `app.state` (created → running → stopping → stopped | error) work, and graceful stop finishes the batch and leaves the group.

**Blocked by:** 02

**Status:** done

- [x] Adds `@platformatic/kafka` (ADR-0004). Consumer group = Application ID
- [x] Integration test harness: tests connect to a broker given by an environment variable. A script downloads and starts a local Kafka 4.x KRaft broker without Docker (see spec Testing Decisions). Each test uses unique topic names
- [x] Batch closed by time or count (commit interval, default 100 ms)
- [x] `exactly-once` and `workers > 0` are rejected as not implemented yet
- [x] Integration test: N records in → all N out

## Implementation notes (from research)

- Copy the tested draft `scripts/kafka-local.sh` (start|stop|status|reset, Kafka 4.4.0 in `~/.cache/kstreams-ts/`, Java 17) from `local-broker.md`.
- Test helpers: use idempotent producers and wait for partition leaders after `createTopics` (a non-idempotent producer wrote duplicates right after topic creation).
- Set a short `sessionTimeout` in tests (the 60 s default blocks reassignment after a crash).
- Client defaults: consumer isolation is read_committed, `consume()` starts at latest, autocommit is on. Set these explicitly.
- Research notes: /Users/igorshalygin/.claude/jobs/e36bc646/tmp/notes/kafka-client.md, local-broker.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/07`. Public API:
  - **Config:** `StreamsAppConfig` gains `brokers?` (needed only for `start`), `guarantee?: DeliveryGuarantee` (default `'at-least-once'`; `'exactly-once'` throws "not implemented yet" in `createStreamsApp`), `workers?` (default 0; `> 0` throws "not implemented yet"), `commitIntervalMs?` (default 100), `commitMaxRecords?` (default 1000), `sessionTimeoutMs?` (default 45000; heartbeat = min(3 s, timeout/3)).
  - **Lifecycle:** `app.state: ApplicationState` (full spec union exported; only created → running → stopping → stopped | error are reported yet), `app.start()` (resolves once the consumer joined the group; rejects without brokers, without sources, when already started/stopped, or on connect failure → `error`), `app.stop()` (graceful: finishes and commits the batch, closes the consumer = LeaveGroup; idempotent; `created` → `stopped`; rejects with the original error if the app is in `error`). No events yet (ticket 25): a fatal error is visible only as `state === 'error'` and via `stop()`.
- **Engine:** `src/worker.ts` `startWorker(graph, options)` is one Worker (ADR-0007): consumer (group = Application ID, `autocommit: false`, `read_committed`, `mode: 'committed'`, `fallbackMode: 'earliest'`), idempotent producer (`acks: all`, Java-compatible murmur2 partitioner for keyed records, round-robin for null keys), one Task per partition number created lazily. `CommitCycle`: read+process records until `commitIntervalMs` or `commitMaxRecords` or a failure/stop → fire wall-clock timers → await all Task work → `producer.send(outputs)` → `consumer.commit(last offset + 1 per partition)`. Outputs are buffered and sent at commit time. The Worker is plain async code with an `onError` callback, so ticket 18 can host it in a worker thread; ticket 15 swaps `#commit` for a transaction; ticket 13 hooks Changelog writes into the Task's collector.
- **Harness:** `scripts/kafka-local.sh` (copied verbatim from research), `npm run kafka -- start|stop|status|reset`; `npm run test:integration` uses `vitest.integration.config.ts` on `test/integration/**` (excluded from `npm test`), skipped with a message unless `KSTREAMS_KAFKA_BROKERS` is set. Helpers in `test/integration/kafka.ts`: unique names, `createTopic` waits for leaders, idempotent test producer, read_committed `consume(topic, n)` with a settle window to catch duplicates, committed offsets, group members.
- **Not done / for later tickets:** no revoke handling — Tasks for revoked partitions are never closed and a commit failing with a rebalance error is fatal (ticket 28); default (eager) assignor, not cooperative-sticky yet (tickets 18/28); no graceful-stop timeout (ticket 28); Task stores never closed; input topics are not checked to exist (ticket 24); ticket 08's harness needs a short `sessionTimeoutMs` in the child app.
