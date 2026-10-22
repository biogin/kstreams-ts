# Windows and time

## Event Time, Stream Time and the Grace Period

Every record carries an **Event Time**: the timestamp on the Kafka record, describing when the event happened. A Task tracks its **Stream Time**, the highest Event Time it has seen; it only moves forward. Windows and Stream Time timers are driven by Stream Time, not by the wall clock.

The **Grace Period** is how far behind Stream Time a record can arrive and still be included in its window. A window closes once Stream Time reaches its end plus the Grace Period; a record for a closed window is dropped and counted (the `late-records-dropped` metric, `lateRecordsDropped` in the Test Driver).

## Tumbling windows

`tumbling(size, { grace })` makes fixed-size, non-overlapping windows aligned to the epoch: a record falls into exactly one window `[start, start + size)`. A size is milliseconds or a string such as `'30s'`, `'1m'`, `'1h'`, `'1d'`. The Grace Period defaults to 0.

```ts
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
```

`count`, `aggregate` and `reduce` are available on a windowed stream. The result is a Table keyed by `Windowed<K>`: `{ key, window: { start, end } }`. Each update is forwarded with the **window end** as its timestamp.

## Windowed keys on topics

On a topic a windowed key is the key Serde's bytes followed by the window start as an 8-byte big-endian signed integer. Use `serdes.windowed(keySerde, size)` to write it, and to read such a topic in another app or service built on this library; the window end is the start plus the size.

## Retention and expiry

A window's state is kept until Stream Time passes its end plus the Grace Period, that is **retention = size + Grace Period**. It is not configurable yet. Windows expire only when new records arrive (after each record is processed; a record skipped by an error handler expires nothing): an idle Task keeps its windows, and a stalled input partition holds its state until data flows again.

Skipped records (by an [error handler](./error-handlers.md)) don't advance Stream Time.
