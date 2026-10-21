# 23: Error handlers

**What to build:** Deserialization and processing errors go to pluggable handlers that fail fast by default. A ready-made `deadLetter(topic)` handler writes inside the transaction under exactly-once. Produce errors and `AsyncNotAllowedError` always stop the app (ADR-0010, ADR-0012).

**Blocked by:** 04, 16

**Status:** done

- [x] Handler config on the Streams App
- [x] `deadLetter(topic)` (transactional under exactly-once)
- [x] Produce errors and `AsyncNotAllowedError` skip handlers

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 18

- Errors from worker threads reach the main thread as plain `Error`s and lose their class (e.g. `DeserializationError`, `AsyncNotAllowedError`). Error handling and `skipsErrorHandlers` must work inside the thread, and errors crossing the boundary should keep a recognizable type/code.

## Comments

- Done on `ticket/23`. **API** (exported from the entry point, `src/error-handlers.ts`): `StreamsAppConfig.errorHandlers?: ErrorHandlers = { deserialization?, processing? }`; `ErrorHandler = (context: ErrorHandlerContext) => 'continue' | 'fail'` (synchronous; a returned thenable or any other answer stops the app with a `TypeError` whose `cause` is the original error; a handler that throws stops the app with its error). `ErrorHandlerContext = { kind, error, record: FailedRecord, produce(topic, HandlerOutputRecord) }`, `FailedRecord = { topic, partition, offset: bigint, key, value (bytes), timestamp }`. `deadLetter(topic)` writes the record as read (bytes, Event Time) with headers `kstreams.error.{kind,name,message,stack,topic,partition,offset}` and continues; the topic name is checked, the topic is not created. Unknown handler keys / non-function handlers throw at `createStreamsApp`.
- **Where:** the handlers live on the `ProcessorGraph` (`graph.errorHandlers`), so the Test Driver and every Worker (worker threads included, since they rebuild the app from the App Module) use them with no extra plumbing. `Task.process(topic, record, offset)` now takes the offset. Handler writes go through the Task's `RecordCollector` like sink outputs, so under exactly-once they join the open Kafka transaction; `OutputRecord` (= `SerializedRecord` + optional `headers`) carries headers to `toMessage` in worker.ts (worker.ts diff: offset + headers only).
- **What skips handlers:** `AsyncNotAllowedError` (via `skipsErrorHandlers`), errors raised by timers (no record to skip), and produce errors, which happen in the committers outside any record's processing and so never reach the Task's handler path. Serialization errors in a sink count as processing errors. A deserialization-skipped record doesn't advance Stream Time.
- **Semantics:** outputs and store writes a record made before it failed are kept (as Kafka Streams). On a processing error the Task waits for all of that record's in-flight async Processor calls to settle before calling the handler, so records never overlap. Side fixes in `task.ts`: an async Processor whose call rejected stayed "busy" with a rejected promise forever (fine under fail-fast, wrong once records can continue); async errors are now collected per record instead of `Promise.all` racing.
- **Worker threads:** errors cross the boundary as a tagged plain object (`transferableError` in the thread, `errorFromThread` on the main side, in `src/worker-thread.ts`): own data properties that structured clone can copy, `name`, `stack` and the `cause` chain are kept, and the error is rebuilt as an instance of its class for `DeserializationError`, `AsyncNotAllowedError` and JS built-ins (others are `Error` with the original `name`). So `instanceof` works on `stop()`'s rejection.
- **Test Driver:** offsets are counted per input topic from 0; `output(topic)` also accepts `deadLetter` topics (readable before anything is written; bytes unless Serdes are passed) and topics a custom handler wrote; output records carry `headers` only when they have some (new type `TestOutputRecord`).
- **Tests:** `test/error-handlers.test.ts` (17, Test Driver routing). `test/integration/error-handlers.test.ts` (6): dead letter under at-least-once and exactly-once (offsets of failed records committed, app keeps running); the exactly-once dead-letter write is in the transaction (a later `AsyncNotAllowedError` in the same batch aborts it: the record is visible read_uncommitted only, no offsets committed); a produce error (output topic `max.message.bytes=100`) stops the app with handlers configured; handlers running in a worker thread plus `AsyncNotAllowedError` / `DeserializationError` (with `topic`, `part`, `timestamp`, `cause`) keeping their class across threads (checked by mutation). Full integration suite passed twice.
- **For 25:** the typed error events should carry the rebuilt error (class kept). Consider metrics/events for records a handler skipped (Kafka Streams counts `dropped-records`); nothing counts them yet. Handler `'continue'` answers are invisible to observability otherwise.
