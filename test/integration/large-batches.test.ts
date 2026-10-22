import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  createStreamsApp,
  type DeliveryGuarantee,
  type StreamsApp,
  serdes,
  stores,
} from '../../src/index.js'
import { readCheckpoint, taskDirectory } from '../support/internals.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('batches larger than a produce request (workers: 0)', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  it.each<DeliveryGuarantee>(['at-least-once', 'exactly-once'])(
    '%s: splits a batch of well over 1 MB to one partition over several requests',
    async (guarantee) => {
      const input = await kafka.createTopic('input', 1)
      const output = await kafka.createTopic('output', 1)
      const applicationId = uniqueName('large')
      kafka.deleteOnClose(`${applicationId}-latest-changelog`)
      const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-large-'))
      directories.push(stateDir)
      const app = createStreamsApp({
        applicationId,
        brokers,
        guarantee,
        stateDir,
        // Every record in one batch: 1500 KB of outputs and as much of Changelog records.
        commitIntervalMs: 5_000,
        commitMaxRecords: 2_000,
        sessionTimeoutMs: 10_000,
        changelogReplicationFactor: 1,
      })
      const latest = app.store('latest', strings, { supplier: stores.sqlite() })
      const keeper = app.processor<string, string | null, string, string | null>(
        'keeper',
        (ctx) => ({
          process(record) {
            ctx.store(latest).put(record.key, record.value ?? '')
            ctx.forward(record)
          },
        }),
      )
      app.stream(input, strings).process(keeper).to(output, strings)
      apps.push(app)

      const now = Date.now()
      const value = 'x'.repeat(1_000)
      const sent: TestRecord[] = Array.from({ length: 1_500 }, (_, i) => ({
        key: `key-${i}`,
        value,
        timestamp: now + i,
      }))
      // In several requests: the test's producer has the same limit.
      for (let i = 0; i < sent.length; i += 300) await kafka.produce(input, sent.slice(i, i + 300))
      await app.start()

      const received = await kafka.consume(output, sent.length, { timeoutMs: 60_000 })
      expect(received.map(({ key }) => key)).toEqual(sent.map(({ key }) => key))
      await eventually(
        async () => (await kafka.committedOffsets(applicationId, input)).get(0) === 1_500n,
        { timeoutMs: 30_000, message: 'every offset committed' },
      )
      expect(app.state).toBe('running')
      await app.stop()

      // The Checkpoint is after the last Changelog record, though it was written by several sends:
      // 1500 records, plus the transaction markers under exactly-once.
      const offset = readCheckpoint(taskDirectory(stateDir, applicationId, 0))?.get('latest')
      if (guarantee === 'at-least-once') expect(offset).toBe(1_500n)
      else expect(offset).toBeGreaterThanOrEqual(1_500n)
    },
    120_000,
  )
})
