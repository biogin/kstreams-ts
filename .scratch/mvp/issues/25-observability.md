# 25: Observability

**What to build:** `app.state` reports the full Application State lifecycle (including rebalancing and restoring), typed events fire for state changes and errors, and a metrics interface reports lag, restore progress, commit duration, aborts, dropped late records and throughput.

**Blocked by:** 19, 30

**Status:** ready-for-agent

- [x] Full Application State lifecycle
- [x] Typed event emitter
- [x] Metrics interface with the listed metrics

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 18

- With worker threads, only running/stopped/failed cross the thread boundary today. Rebalancing/restoring Application States and metrics need Workers to report state and metrics to the main thread.

## Notes from ticket 23

- Count and report records skipped by error handlers (a dropped-records metric, as in Kafka Streams), and emit typed error events carrying the rebuilt error class from worker threads.
- Records skipped by a handler do not advance Stream Time; outputs a record forwarded before it failed are kept (as in Kafka Streams). Document both in the user docs (ticket 26).

## Notes from ticket 29

- Rebalance rounds are visible in Ownership (src/ownership.ts). Waits on Task locks, unstable offsets and open Changelog transactions are silent today; expose them as metrics/events.

## Comments

- 2026-10-22: Done on branch `ticket/25`.
  - **API (all in `src/observability.ts`, exported from the index, documented in doc comments):** `ApplicationState` moved there. `app.on(event, listener)` / `app.off(...)` (typed by `StreamsAppEvents`; own tiny emitter, not Node's `EventEmitter`, so an `error` event without a listener doesn't throw; listener exceptions are ignored). Events: `state` `{ state, previous }`, `error` `{ error }` (root cause only, once; same object `stop()` rejects with; the worker-thread rebuilt class is kept), `rebalance` `{ worker, assigned, revoked }` (each completed round; each Worker's first one reports its initial assignment), `restore-start` `{ worker, task, store, full, remaining }`, `restore-progress` `{ ..., restored, remaining }`, `restore-end` `{ ..., restored, durationMs, cancelled }`. Config `metrics?: MetricsSink` with one method `record(metric: Metric)`; `Metric` is a union discriminated by `name`, with `type: 'counter' | 'gauge' | 'duration'` and typed `labels` (worker, task, topic/partition, store, kind, reason, wait). Metrics: `records-processed`, `records-dropped` (kind = deserialization/processing), `late-records-dropped`, `aborts` (reason = rebalance/error/shutdown-timeout; exactly-once only, only when a transaction was open), `commit-duration` (not for empty batches), `consumer-lag` (committed offset → LSO, measured at most every 2 s in the background, needs 2–3 offset requests per Worker), `records-restored`, `restore-remaining`, `wait-duration` (wait = task-lock/stable-offsets/changelog-transactions). Records per second derive from the `records-processed` counter; there is no rate metric.
  - **Aggregation (Runtime in streams-app.ts):** `start()` → `rebalancing` (every Worker starts as rebalancing until it reports). Then: any Worker `rebalancing` ⇒ rebalancing; else any `restoring` ⇒ restoring; else running. Reports are ignored once the app is stopping/stopped/error. `start()` now resolves once every Worker has joined the group (the app may still be rebalancing/restoring), no longer forcing `running`.
  - **Worker state (CommitCycle in worker.ts):** `rebalancing` between the client's `consumer:group:rebalance` and `consumer:group:join`; else `restoring` while a Task *with State Stores* is starting (lock wait + stable offsets + Restore; exactly-once without stores doesn't count, its start is only the stable-offset wait); else `running`. Updated in `#start`, `#takeStarted`, `#cancelStarts`. Since a Task starts when its first record arrives, a moved Task without new records doesn't make the app `restoring`.
  - **Task counters:** `Task.metrics()` now has `recordsProcessed` (finished with: processed or skipped), `recordsDropped` by kind, `lateRecordsDropped`. A Worker reports deltas only after a successful commit (records of an aborted batch are counted when processed again; counts of a Task closed before its batch committed are not reported).
  - **Worker threads:** a new `reports` message carries `WorkerReport`s (state/event/metric), batched per `setImmediate`, flushed before any other message so the order holds. Threads collect metrics only if the app config has a sink (the rebuilt app has the same config); the thread's own sink is never called.
  - **Test Driver:** `metrics()` is now `{ recordsProcessed, recordsDropped, lateRecordsDropped }`, and the app's sink gets the counter metrics as Worker 0 / Task 0 after each pipe/advance.
  - **Restore hooks (restore.ts):** `ChangelogReader.read` takes an optional `ReadObserver` (waited/reading/advanced), `restoreStores` an optional `RestoreListener`.
  - **Tests:** unit `test/observability.test.ts` (event and metric types, state events, listener exceptions, sink validation, Test Driver counters incl. both error kinds and late records). Integration `test/integration/observability.test.ts`: (1) exactly-once, workers: 0, 2 partitions, SQLite: state sequence starts created → rebalancing and contains restoring, ends stopping → stopped; rebalance assigned both partitions; restore-start/end per Task (full, 0 remaining); wait-duration for all three waits per Task; commit-duration; no aborts; consumer lag reaches 0; records-processed = 200. Second run on a new state dir: full Restore of exactly 200 Changelog records (events and `records-restored`), `restore-remaining` 0. (2) workers: 2 via the App Module fixture: records-processed = 100 across threads, rebalances from both Workers, restore events for all 4 Tasks, commit-duration from Worker 1, then a failing record → one `error` event with a `RangeError` (class kept), `stop()` rejects with the same object, one `aborts{reason: 'error'}`. Changed: `non-blocking-restore.test.ts` now expects `restoring` while Task 1 restores (was `running`); `workers.test.ts` waits for `running` instead of asserting it right after `start()`.
  - **For 26 (docs):** document the lifecycle and aggregation rules (and that `start()` resolves before `running`), readiness = `running` (or `restoring` if partial availability is fine), liveness = not `error`; the event list and that listener/sink exceptions are ignored; the metric table with types/labels and the lag definition (committed offset to LSO, every 2 s); records skipped by handlers don't advance Stream Time and outputs forwarded before a failure are kept (from 23); a Task starts (restores) on its first record; no limit on concurrent Restores at startup (from 19). An OpenTelemetry adapter can map `type` to counter/gauge/histogram generically.

