# Observability: states, events and metrics

## Application State

`app.state` reports the **Application State**:

```text
created -> rebalancing <-> restoring <-> running -> stopping -> stopped
                  (any state) -> error
```

| State | Meaning |
| --- | --- |
| `created` | Defined, not started. |
| `rebalancing` | Some Worker is joining the consumer group or taking part in a rebalance (from `start()` until every Worker has joined, and whenever the group rebalances). |
| `restoring` | No Worker is rebalancing, and some Worker has a Task with State Stores that is starting: waiting for its previous owner to let it go, or restoring its stores from their Changelogs. The Worker's other Tasks keep processing. |
| `running` | Every Worker has joined and none is rebalancing or restoring. |
| `stopping`, `stopped` | After `stop()`. |
| `error` | A Worker failed; the app stopped the others. |

With several Workers the state is aggregated: any Worker rebalancing means `rebalancing`; otherwise any restoring means `restoring`; otherwise `running`. A Task restores when its **first record arrives**, so an app can go `running` and later `restoring` as data reaches an idle Task. `start()` resolves once every Worker has joined, possibly before `running`.

**Probes.** Readiness is `app.state === 'running'`; liveness is `app.state !== 'error'`. Because a restoring app is alive but not ready, do not use liveness to restart an app that is restoring a large store.

## Events

Add listeners with `app.on(event, listener)` and remove them with `app.off`. They run synchronously, in the main thread (events of worker threads are passed on), in the order added. An exception a listener throws is ignored.

| Event | Payload | When |
| --- | --- | --- |
| `state` | `{ state, previous }` | The Application State changed. |
| `error` | `{ error }` | The app stopped because of this error (the root cause, emitted once). `stop()` rejects with the same error. |
| `rebalance` | `{ worker, assigned, revoked }` | A Worker took part in a completed rebalance round. A cooperative rebalance that moves Tasks has two rounds: the giver sees them revoked in the first, the taker sees them assigned in the second. |
| `restore-start` | `{ worker, task, store, full, remaining }` | A Task began restoring a store. `full` is true when the store was wiped and it is restored from the whole Changelog. |
| `restore-progress` | `{ worker, task, store, restored, remaining }` | Another chunk of the Changelog was applied. |
| `restore-end` | `{ worker, task, store, restored, durationMs, cancelled }` | A Restore finished, or was cancelled because the Task moved or the app is stopping (the store is then incomplete). |

`remaining` counts Changelog offsets; transaction markers and aborted records take offsets too, so fewer records may be left.

```ts
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
```

```ts
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
```

## Metrics

Pass `metrics: { record(metric) {} }` in the config to receive a `Metric` for every measurement, as it happens. Without a sink nothing is collected. `record` runs synchronously from the engine, so keep it quick (update an in-memory registry); an exception it throws is ignored. With worker threads, every Worker's metrics reach the sink in the main thread. Labels are strings and numbers, so a generic sink can pass `Object.entries(metric.labels)` on.

A metric's `type` says how to aggregate it: `counter` (`value` is an increment), `gauge` (`value` replaces the previous one with the same labels), `duration` (`value` is one observation in milliseconds, for a histogram).

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `records-processed` | counter | `worker`, `task` | Input records a Task finished with (processed, or skipped by an error handler). Counted once their batch has committed; the records of an aborted batch are counted when processed again. Throughput is the rate of this counter. |
| `records-dropped` | counter | `worker`, `task`, `kind` | Records an error handler skipped (`kind`: `deserialization` or `processing`). |
| `late-records-dropped` | counter | `worker`, `task` | Records windowed aggregations dropped for arriving after the Grace Period. |
| `aborts` | counter | `worker`, `reason` | Kafka transactions aborted (exactly-once). `reason`: `rebalance`, `error`, `shutdown-timeout` or `transaction-timeout` (the batch outlived `transactionTimeoutMs` and is processed again). |
| `commit-duration` | duration | `worker` | How long committing a batch took: producing or flushing outputs, committing offsets and the transaction, store transactions and Checkpoints. Not reported for a batch with nothing to commit. |
| `consumer-lag` | gauge | `worker`, `topic`, `partition` | Records of an owned input partition from its committed offset to its last stable offset (as `read_committed` readers see it). Sampled every 2 seconds. |
| `records-restored` | counter | `worker`, `task`, `store` | Changelog records a Restore applied. |
| `restore-remaining` | gauge | `worker`, `task`, `store` | Changelog offsets a Restore has left; 0 once done. |
| `wait-duration` | duration | `worker`, `task`, `wait` | How long a starting Task waited for something its previous owner held. `wait`: `task-lock` (its Task directory lock), `stable-offsets` (exactly-once: its committed offsets to become stable, that is the previous owner's transaction to end) or `changelog-transactions` (open transactions on a Changelog partition to end, before Restore). |

`worker` is the Worker's number (0 to the thread count minus 1; 0 with `workers: 0`) and `task` is the partition number.

Consumer lag is measured up to the last stable offset, so an open transaction on an input topic holds it back. Alert on lag that keeps growing, not on its absolute value.
