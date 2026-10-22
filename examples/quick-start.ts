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
  .groupByKey()
  .count({ store: 'views-per-user' })
  .toStream()
  .to('views-per-user', { key: serdes.string, value: serdes.number })

export default app
