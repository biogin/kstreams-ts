import { createStreamsApp, serdes } from 'kstreams-ts'

const app = createStreamsApp({
  applicationId: 'page-views',
  brokers: ['localhost:9092'],
  stateDir: './state',
  guarantee: 'exactly-once',
  changelogReplicationFactor: 1, // 3 (the default) needs at least three brokers
})

const view = serdes.json<{ path: string }>()

app
  .stream('page-views', { key: serdes.string, value: view })
  .filter((_userId, value): value is { path: string } => value !== null)
  .map((_userId, value) => ({ key: value.path, value }))
  .groupByKey({ key: serdes.string, value: view })
  .count({ store: 'views-per-path' })
  .toStream()
  .to('views-per-path', { key: serdes.string, value: serdes.number })

export default app
