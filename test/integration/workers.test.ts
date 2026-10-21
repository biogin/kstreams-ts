import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import type { WorkersCountParams } from './fixtures/workers-count-app.js'
import { type AppProcess, launchAppModule } from './harness/app-process.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

// Worker threads run the App Module from the TypeScript sources, which needs the `.js` → `.ts`
// resolve hook; so the app runs in an App Process (child process), like the crash tests.
const workersCountApp = new URL('./fixtures/workers-count-app.ts', import.meta.url)

describeWithKafka('worker threads on real Kafka (exactly-once, workers: 2)', () => {
  let kafka: ReturnType<typeof createKafka>
  const processes: AppProcess[] = []
  const directories: string[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(processes.splice(0).map((app) => app.kill()))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  async function launch(params: Omit<WorkersCountParams, 'brokers' | 'stateDir'>) {
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-workers-'))
    directories.push(stateDir)
    const app = await launchAppModule(workersCountApp, { ...params, brokers, stateDir })
    kafka.deleteOnClose(`${params.applicationId}-counts-changelog`)
    processes.push(app)
    return app
  }

  const now = Date.now()
  const keys = Array.from({ length: 12 }, (_, i) => `key-${i}`)

  function records(count: number, from = 0): TestRecord[] {
    return Array.from({ length: count }, (_, i) => ({
      key: keys[(from + i) % keys.length] ?? null,
      value: `value-${from + i}`,
      timestamp: now + from + i,
    }))
  }

  it('runs 2 Workers in one process that share the Tasks, with exact counts', async () => {
    const input = await kafka.createTopic('workers-input', 4)
    const output = await kafka.createTopic('workers-output', 4)
    const applicationId = uniqueName('workers')
    // Some records wait before the Workers join, some arrive once both run.
    await kafka.produce(input, records(300))

    const app = await launch({ applicationId, input, output, workers: 2 })
    // `start()` resolved: both Workers are members of the group.
    expect(await kafka.groupMembers(applicationId)).toBe(2)
    await kafka.produce(input, records(300, 300))

    // Exactly-once: every key's counts are 1, 2, ..., 50, each once, in order.
    const received = await kafka.consume(output, 600, { timeoutMs: 40_000, settleMs: 1_500 })
    for (const key of keys) {
      const counts = received.filter((record) => record.key === key).map(({ value }) => value)
      expect(counts, key).toEqual(Array.from({ length: 50 }, (_, i) => String(i + 1)))
    }

    expect(await app.stop()).toEqual({ code: 0, signal: null })
    await eventually(async () => (await kafka.groupMembers(applicationId)) === 0, {
      message: 'both Workers leaving the group',
    })
  })

  it('goes to error when one Worker fails, and stops the other one', async () => {
    const input = await kafka.createTopic('workers-input', 2)
    const output = await kafka.createTopic('workers-output', 2)
    const applicationId = uniqueName('workers-fail')
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-workers-'))
    directories.push(stateDir)
    const params: WorkersCountParams = {
      applicationId,
      brokers,
      input,
      output,
      workers: 2,
      stateDir,
      failOn: 'boom',
    }
    // In this process this time, to watch the Application State (the test process preloads the
    // resolve hook, see vitest.integration.config.ts). The worker threads read the params from
    // the environment too.
    process.env.KSTREAMS_APP_PARAMS = JSON.stringify(params)
    const { default: app } = await import('./fixtures/workers-count-app.js')
    if (app === undefined) throw new Error('The App Module exports no app')
    try {
      await app.start()
      expect(app.state).toBe('running')
      kafka.deleteOnClose(`${applicationId}-counts-changelog`)
      expect(await kafka.groupMembers(applicationId)).toBe(2)

      await kafka.produce(input, [{ key: 'k', value: 'boom', timestamp: now }])
      await eventually(() => app.state === 'error', { message: 'the error state' })
      // Nobody called stop(): the app stopped the healthy Worker itself.
      await eventually(async () => (await kafka.groupMembers(applicationId)) === 0, {
        message: 'both Workers leaving the group',
      })
    } finally {
      await expect(app.stop()).rejects.toThrow('Processor failed on purpose')
      delete process.env.KSTREAMS_APP_PARAMS
    }
  })

  it('fails to start when the Worker threads cannot rebuild the app', async () => {
    const input = await kafka.createTopic('workers-input', 2)
    const output = await kafka.createTopic('workers-output', 2)
    const applicationId = uniqueName('workers-broken')

    await expect(
      launch({ applicationId, input, output, workers: 2, noAppInWorkers: true }),
    ).rejects.toThrow(/must export the Streams App as its default export/)
    expect(await kafka.groupMembers(applicationId)).toBe(0)
  })
})
