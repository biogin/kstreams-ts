import { createStreamsApp, serdes, stores } from 'kstreams-ts'

const app = createStreamsApp({ applicationId: 'cache', stateDir: './state' })

app
  .stream('in', { key: serdes.string, value: serdes.string })
  .groupByKey()
  // Keep this aggregation in memory instead of SQLite; every start is then a full Restore.
  .count({ store: 'counts', storeSupplier: stores.inMemory() })
  .toStream()
  .to('out', { key: serdes.string, value: serdes.number })

export default app
