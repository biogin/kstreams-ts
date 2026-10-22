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

export default app
