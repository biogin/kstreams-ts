# The Processor API

The **Processor API** is the low-level surface the [DSL](./dsl.md) compiles to. Use it when the DSL doesn't cover your logic: custom state, timers, async I/O, or emitting records yourself.

A **Processor** is a unit of per-record logic that reads and writes State Stores, schedules timers and forwards records downstream. You register it by name with `app.processor(name, supplier)`. The supplier is called once per Task and returns a `Processor`; connect it to a Stream with `stream.process(processor)`, which returns the Stream of records it forwards.

```ts
import {
  createStreamsApp,
  type Processor,
  type ProcessorContext,
  type StreamRecord,
  serdes,
} from 'kstreams-ts'

const app = createStreamsApp({ applicationId: 'dedupe' })
const strings = { key: serdes.string, value: serdes.string }

// A State Store, shared by name with the Processors that use it. Each Task gets its own copy.
const seen = app.store('seen', { key: serdes.string, value: serdes.number })
```

```ts
// A Processor: one instance per Task. `process` may be async; the Task awaits it before it hands
// over the next record, so records of a Task never overlap.
const dedupe = app.processor('dedupe', (ctx: ProcessorContext<string, string | null>) => {
  const store = ctx.store(seen)

  // A timer: every minute of Stream Time, delete the entries older than an hour.
  ctx.schedule(60_000, 'stream-time', (now) => {
    for (const [key, at] of store.all()) {
      if (at < now - 3_600_000) store.delete(key)
    }
  })

  const processor: Processor<string, string | null> = {
    async process(record: StreamRecord<string, string | null>) {
      if (record.value === null || store.get(record.key) !== undefined) return
      store.put(record.key, record.timestamp)
      ctx.forward({ key: record.key, value: record.value, timestamp: record.timestamp })
    },
  }
  return processor
})

app.stream('events', strings).process(dedupe).to('unique-events', strings)
```

## The context

- `ctx.forward({ key, value, timestamp })` sends a record to every child. There is no need to await it. Always set a `timestamp` (usually the input record's, or in a timer the one the callback receives); forwarded records don't get one by themselves.
- `ctx.store(store)` opens a State Store for this Task; pass the value `app.store` returned for typed access, or its name. See [State Stores](./state-stores.md).
- `ctx.schedule(intervalMs, type, callback)` schedules a recurring timer and returns a `Timer` with `cancel()`.

Records carry Tombstones: a Processor reading a topic sees `null` values and decides what they mean.

## Async Processors

`process` and timer callbacks may be async, for example to call an HTTP service. The Task awaits each call before the next record, so per-key order is kept. This has costs, see [async Processors stretch transactions](./delivery-guarantees.md#async-processors-stretch-transactions): slow I/O keeps the batch's Kafka transaction open longer.

## Timers

- `'stream-time'` timers fire as the Task's Stream Time advances. One scheduled before the Task saw a record starts counting from the first record's Event Time. Because Stream Time only moves with records, an idle partition fires no Stream Time timers.
- `'wall-clock-time'` timers fire as wall-clock time passes.

If time jumps over several intervals, the timer fires once, with the current time. Timers fire between records, never during one, and are not persisted: after a restart, or when a rebalance moves a Task to another process, the new Task is created and its supplier schedules them again. A Task kept across a cooperative rebalance keeps its timers. Errors raised by a timer callback stop the app; they aren't given to [error handlers](./error-handlers.md), because there is no record to skip.

## Wiring several Processors

Connecting several Streams to one Processor merges them. Calling `.process` on a Processor's output chains Processors. A Processor's output Stream has no Serdes, so `.to()` after it needs both unless you pass them as above.
