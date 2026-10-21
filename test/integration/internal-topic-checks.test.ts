import { Admin } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { createStreamsApp, type StreamsApp, serdes } from '../../src/index.js'
import { brokers, createKafka, describeWithKafka, uniqueName } from './kafka.js'

const strings = { key: serdes.string, value: serdes.string }

describeWithKafka('Internal Topic checks on real Kafka', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  /** A counting app (one State Store, so one Changelog) reading `input`. */
  function countApp(input: string, autoCreateInternalTopics?: boolean): StreamsApp {
    const app = createStreamsApp({
      applicationId: uniqueName('internal-checks'),
      brokers,
      sessionTimeoutMs: 10_000,
      changelogReplicationFactor: 1,
      ...(autoCreateInternalTopics === undefined ? {} : { autoCreateInternalTopics }),
    })
    app.stream(input, strings).groupByKey({ key: serdes.string }).count({ store: 'counts' })
    apps.push(app)
    return app
  }

  async function withAdmin<T>(run: (admin: Admin) => Promise<T>): Promise<T> {
    const admin = new Admin({
      clientId: uniqueName('admin'),
      bootstrapBrokers: brokers,
    })
    try {
      return await run(admin)
    } finally {
      await admin.close()
    }
  }

  /** The partition count of `topic`, or undefined if it doesn't exist. */
  function partitionsOf(topic: string): Promise<number | undefined> {
    return withAdmin(async (admin) => {
      if (!(await admin.listTopics()).includes(topic)) return undefined
      const metadata = await admin.metadata({
        topics: [topic],
        forceUpdate: true,
      })
      return metadata.topics.get(topic)?.partitionsCount
    })
  }

  function createInternalTopic(topic: string, partitions: number): Promise<void> {
    kafka.deleteOnClose(topic)
    return withAdmin(async (admin) => {
      await admin.createTopics({
        topics: [topic],
        partitions,
        replicas: 1,
        configs: [{ name: 'cleanup.policy', value: 'compact' }],
      })
    })
  }

  it('creates the Changelogs by default', async () => {
    const input = await kafka.createTopic('in', 2)
    const app = countApp(input)
    kafka.deleteOnClose(`${app.applicationId}-counts-changelog`)
    await app.start()
    expect(await partitionsOf(`${app.applicationId}-counts-changelog`)).toBe(2)
  })

  it('lists the missing Internal Topics when auto-creation is off, and creates none', async () => {
    const input = await kafka.createTopic('in', 3)
    const app = countApp(input, false)
    const started = app.start()
    await expect(started).rejects.toThrow(`"${app.applicationId}-counts-changelog"`)
    await expect(started).rejects.toThrow(/3 partitions/)
    await expect(started).rejects.toThrow(/cleanup\.policy=compact/)
    await expect(started).rejects.toThrow(/autoCreateInternalTopics/)
    expect(app.state).toBe('error')
    expect(await partitionsOf(`${app.applicationId}-counts-changelog`)).toBeUndefined()
    expect(await kafka.groupMembers(app.applicationId)).toBe(0)
  })

  it('starts with auto-creation off once the Internal Topics exist', async () => {
    const input = await kafka.createTopic('in', 2)
    const app = countApp(input, false)
    await createInternalTopic(`${app.applicationId}-counts-changelog`, 2)
    await app.start()
    expect(app.state).not.toBe('error')
  })

  it.each([true, false])(
    'fails on an Internal Topic with the wrong partition count (auto-creation %s)',
    async (auto) => {
      const input = await kafka.createTopic('in', 3)
      const app = countApp(input, auto)
      const changelog = `${app.applicationId}-counts-changelog`
      await createInternalTopic(changelog, 2)
      const started = app.start()
      await expect(started).rejects.toThrow(`"${changelog}" has 2 partitions`)
      await expect(started).rejects.toThrow(/needs 3/)
      expect(app.state).toBe('error')
    },
  )

  it('reports every missing input topic in one clear error', async () => {
    const present = await kafka.createTopic('in', 2)
    const missingA = uniqueName('absent-a')
    const missingB = uniqueName('absent-b')
    const app = createStreamsApp({
      applicationId: uniqueName('internal-checks'),
      brokers,
      changelogReplicationFactor: 1,
    })
    app.stream(present, strings).to(uniqueName('out'), strings)
    app.stream(missingA, strings).to(uniqueName('out'), strings)
    app.stream(missingB, strings).to(uniqueName('out'), strings)
    apps.push(app)
    const started = app.start()
    await expect(started).rejects.toThrow(/input topics do not exist/)
    await expect(started).rejects.toThrow(`"${missingA}"`)
    await expect(started).rejects.toThrow(`"${missingB}"`)
    await expect(started).rejects.not.toThrow(`"${present}"`)
    expect(app.state).toBe('error')
  })
})
