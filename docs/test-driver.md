# The Test Driver

The **Test Driver** runs a Streams App's Processors and State Stores in memory, without Kafka, as part of the public API. Records go through the app's real Serdes on the way in and out, so serialization bugs show up in unit tests.

Export your app from a module without side effects (see [the App Module](./workers.md#the-app-module)) so tests can import it without starting it. Here is a test for the windowed count from [windows](./windows.md):

```ts
import assert from 'node:assert/strict'
import { createTestDriver } from 'kstreams-ts'
import app from './windowed-counts.js'

const driver = createTestDriver(app)
const input = driver.input<string, string>('clicks')
const output = driver.output('clicks-per-minute')

// Records go through the real Serdes. `timestamp` is the Event Time.
await input.pipe('alice', 'home', { timestamp: 10_000 })
await input.pipe('alice', 'cart', { timestamp: 20_000 })
assert.deepEqual(
  output.readAll().map((record) => record.value),
  [1, 2],
)
```

```ts
// The State Store behind the aggregation can be read directly.
console.log(driver.store('clicks-per-minute').all())

// A record far behind Stream Time, past the Grace Period, is dropped and counted.
await input.pipe('alice', 'home', { timestamp: 300_000 })
await input.pipe('alice', 'home', { timestamp: 10_000 })
assert.equal(driver.metrics().lateRecordsDropped, 1)

// Wall-clock timers fire when you advance the clock.
await driver.advanceWallClockTime(60_000)
```

## Reference

- `driver.input(topic, serdes?)` returns a topic to `pipe(key, value, { timestamp })`. A null value is a Tombstone. Without a `timestamp`, the Test Driver's wall-clock time is used (it starts at 0). Pass your own `serdes` to feed the app malformed bytes.
- `driver.output(topic, serdes?)` returns a topic to `read()` (the oldest unread record; throws if none) or `readAll()`. It works for the app's output topics and for [dead-letter](./error-handlers.md) topics (read as bytes unless you pass Serdes). Records have `key`, `value`, `timestamp` and, if any, `headers`.
- `driver.store(nameOrStore)` reads a State Store (`get`, `all`, `range`); for a `count()` use the name you gave it.
- `driver.advanceWallClockTime(ms)` moves the clock and fires due wall-clock timers. Wall-clock time never affects Stream Time.
- `driver.metrics()` returns `recordsProcessed`, `recordsDropped` and `lateRecordsDropped`. A [metrics sink](./observability.md) on the app receives these counters too, as Worker 0, Task 0.

`pipe` returns a Promise. With only synchronous Processors the record is fully processed before it returns, but `await` it anyway: with async Processors it resolves when processing is done and rejects if it failed. Records piped without awaiting are queued and processed one at a time.

## What it does not do

The Test Driver has one Task (Task 0) and no Kafka. It ignores `stateDir` unless you pass `createTestDriver(app, { stateDir })` (stores are then SQLite files under it), and it does not model rebalances, Restore, transactions or partitioning. Those are covered by the library's integration and crash tests; run your own integration tests against a real broker for them. Errors follow the app's [error handlers](./error-handlers.md); without one, the first error stops the driver and every later `pipe` rejects.
