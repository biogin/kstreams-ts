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

// The State Store behind the aggregation can be read directly.
console.log(driver.store('clicks-per-minute').all())

// A record far behind Stream Time, past the Grace Period, is dropped and counted.
await input.pipe('alice', 'home', { timestamp: 300_000 })
await input.pipe('alice', 'home', { timestamp: 10_000 })
assert.equal(driver.metrics().lateRecordsDropped, 1)

// Wall-clock timers fire when you advance the clock.
await driver.advanceWallClockTime(60_000)
