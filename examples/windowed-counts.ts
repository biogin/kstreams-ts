import { createStreamsApp, serdes, tumbling } from 'kstreams-ts'

const app = createStreamsApp({ applicationId: 'clicks' })

app
  .stream('clicks', { key: serdes.string, value: serdes.string })
  .groupByKey()
  .window(tumbling('1m', { grace: '30s' }))
  .count({ store: 'clicks-per-minute' })
  .toStream()
  .to('clicks-per-minute', { key: serdes.windowed(serdes.string, '1m'), value: serdes.number })

export default app
