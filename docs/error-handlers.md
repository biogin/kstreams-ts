# Error handlers

By default, **the first deserialization or processing error stops the app** (fail-fast, [ADR-0010](./adr/0010-fail-fast-error-handling.md)): bad data and bugs are never silently ignored. To keep going past bad records, configure `errorHandlers`.

```ts
import { createStreamsApp, deadLetter, type ErrorHandler, serdes } from 'kstreams-ts'

// Custom handler: skip records that fail with a RangeError, but stop the app for anything else.
const rejectBadAmounts: ErrorHandler = ({ error, record, produce }) => {
  if (!(error instanceof RangeError)) return 'fail'
  produce('payments-rejected', {
    key: record.key,
    value: record.value,
    timestamp: record.timestamp,
  })
  return 'continue'
}

const app = createStreamsApp({
  applicationId: 'payments',
  errorHandlers: {
    deserialization: deadLetter('payments-dlq'),
    processing: rejectBadAmounts,
  },
})
```

## Kinds

- `deserialization`: a record's key or value can't be read by the input topic's Serde (a `DeserializationError`).
- `processing`: a DSL function, a Processor or an output Serde threw while processing a record.

A handler receives `{ kind, error, record, produce }` and returns `'continue'` (skip the record) or `'fail'` (stop the app with the error). It must be synchronous; if it throws, or returns a Promise or anything else, the app stops. `record` is the failed record as read: topic, partition, offset (a bigint), key and value bytes, and Event Time.

## deadLetter

`deadLetter(topic)` writes each failed record to `topic` unchanged (key, value and Event Time as read) and continues. It sets headers telling what failed: `kstreams.error.kind`, `kstreams.error.name`, `kstreams.error.message`, `kstreams.error.stack`, `kstreams.error.topic`, `kstreams.error.partition` and `kstreams.error.offset`. **The dead-letter topic is not created for you.** Under exactly-once the dead-letter record is written in the same transaction as the batch, so it is committed exactly when the failed record's offset is.

`produce(topic, record)` in a custom handler works the same way (and is how you write to another topic).

## What handlers do not catch

- **Produce errors** (the broker rejecting a write) always stop the app, so the guarantee can never be broken silently.
- **`AsyncNotAllowedError`** (a DSL function returned a Promise) always stops the app; a code bug should never route every record to a dead-letter topic.
- **Errors raised by timers**, which have no record to skip.

## Semantics to know

- A skipped record **does not advance Stream Time**.
- Outputs and State Store writes the record made **before** its processing failed are **kept**; a handler that continues skips only the rest of that record's processing.
- Skipped records are counted in the `records-dropped` metric, labelled by `kind`.
- With worker threads, handlers run in each worker thread.
