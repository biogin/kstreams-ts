import { createStreamsApp, serdes } from 'kstreams-ts'

// The App Module: every worker thread imports this file and rebuilds the app from its default
// export. It must have no side effects, so it must not call `start()`.
const app = createStreamsApp({
  applicationId: 'word-count',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  stateDir: process.env.STATE_DIR ?? './state',
  guarantee: 'exactly-once',
  workers: { count: 4, module: import.meta.url },
})

app
  .stream('lines', { key: serdes.string, value: serdes.string })
  .groupByKey()
  .count({ store: 'counts' })
  .toStream()
  .to('counts', { key: serdes.string, value: serdes.number })

export default app
