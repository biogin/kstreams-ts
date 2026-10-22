import { createStreamsApp, type Metric, type MetricsSink, serdes } from 'kstreams-ts'

// A metrics sink: keep the latest gauge values and sum the counters.
const gauges = new Map<string, number>()
const counters = new Map<string, number>()
const seriesOf = (metric: Metric) => `${metric.name}${JSON.stringify(metric.labels)}`

const sink: MetricsSink = {
  record(metric) {
    const series = seriesOf(metric)
    if (metric.type === 'counter') counters.set(series, (counters.get(series) ?? 0) + metric.value)
    else if (metric.type === 'gauge') gauges.set(series, metric.value)
    // 'duration': feed `metric.value` (milliseconds) into a histogram.
  },
}

const app = createStreamsApp({ applicationId: 'observed', metrics: sink })
app.stream('in', { key: serdes.string, value: serdes.string }).to('out')

app.on('state', ({ state, previous }) => console.log(`state ${previous} -> ${state}`))
app.on('error', ({ error }) => console.error('stopped by', error))
app.on('rebalance', ({ worker, assigned, revoked }) =>
  console.log(`worker ${worker} owns ${assigned.length} partitions, lost ${revoked.length}`),
)
app.on('restore-end', ({ task, store, restored, durationMs, cancelled }) =>
  console.log(`task ${task} restored ${store}: ${restored} records in ${durationMs} ms`, cancelled),
)

// Probes: readiness is `running`, liveness is anything but `error`.
export const ready = () => app.state === 'running'
export const alive = () => app.state !== 'error'

export default app
