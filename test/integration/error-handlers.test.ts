import { afterEach, beforeEach, expect, it } from 'vitest'
import {
  AsyncNotAllowedError,
  type DeliveryGuarantee,
  DeserializationError,
  type StreamsApp,
} from '../../src/index.js'
import type { ErrorHandlersAppParams } from './fixtures/error-handlers-app.js'
import {
  brokers,
  createKafka,
  describeWithKafka,
  eventually,
  type TestRecord,
  uniqueName,
} from './kafka.js'

describeWithKafka('error handlers on real Kafka', () => {
  let kafka: ReturnType<typeof createKafka>
  const apps: StreamsApp[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(apps.splice(0).map((app) => app.stop()))
    await kafka.close()
  })

  const now = Date.now()
  let imports = 0

  /**
   * The app of fixtures/error-handlers-app.ts, imported afresh in this process (worker threads
   * import it themselves). The test process preloads the resolve hook worker threads need.
   */
  async function errorHandlersApp(params: ErrorHandlersAppParams): Promise<StreamsApp> {
    process.env.KSTREAMS_APP_PARAMS = JSON.stringify(params)
    const module = (await import(
      /* @vite-ignore */ new URL(
        `./fixtures/error-handlers-app.ts?app=${++imports}`,
        import.meta.url,
      ).href
    )) as {
      default: StreamsApp
    }
    apps.push(module.default)
    return module.default
  }

  const value = (v: string, flags: { async?: boolean; fail?: boolean } = {}) =>
    JSON.stringify({ value: v, ...flags })

  async function committedTotal(applicationId: string, topic: string): Promise<bigint> {
    const committed = await kafka.committedOffsets(applicationId, topic)
    return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
  }

  for (const guarantee of ['at-least-once', 'exactly-once'] satisfies DeliveryGuarantee[]) {
    it(`sends failed records to the dead-letter topic and goes on (${guarantee})`, async () => {
      const input = await kafka.createTopic('dlq-input', 2)
      const output = await kafka.createTopic('dlq-output', 2)
      const dlq = await kafka.createTopic('dlq', 1)
      const applicationId = uniqueName(`dlq-${guarantee}`)
      const app = await errorHandlersApp({
        applicationId,
        brokers,
        input,
        output,
        guarantee,
        workers: 0,
        deadLetter: dlq,
      })
      await app.start()

      const sent: TestRecord[] = [
        { key: 'a', value: value('a1'), timestamp: now, partition: 1 },
        { key: 'b', value: '{"value":', timestamp: now + 1, partition: 0 },
        { key: 'c', value: value('c1', { fail: true }), timestamp: now + 2, partition: 1 },
        { key: 'd', value: value('d1'), timestamp: now + 3, partition: 0 },
      ]
      await kafka.produce(input, sent)

      const outputs = await kafka.consume(output, 2)
      expect(outputs.map(({ value }) => value).sort()).toEqual(['a1', 'd1'])
      const dead = await kafka.consumeWithHeaders(dlq, 2)
      expect(dead.map(({ key }) => key).sort()).toEqual(['b', 'c'])
      const byKey = new Map(dead.map((record) => [record.key, record]))
      expect(byKey.get('b')).toMatchObject({
        value: '{"value":',
        timestamp: now + 1,
        headers: {
          'kstreams.error.kind': 'deserialization',
          'kstreams.error.name': 'DeserializationError',
          'kstreams.error.topic': input,
          'kstreams.error.partition': '0',
          'kstreams.error.offset': '0',
        },
      })
      expect(byKey.get('c')).toMatchObject({
        value: value('c1', { fail: true }),
        timestamp: now + 2,
        headers: {
          'kstreams.error.kind': 'processing',
          'kstreams.error.message': 'Processing failed on purpose',
          'kstreams.error.partition': '1',
        },
      })
      await eventually(async () => (await committedTotal(applicationId, input)) === 4n, {
        message: 'every offset committed, the failed records included',
      })
      expect(app.state).toBe('running')
      await app.stop()
    })
  }

  it('writes the dead-letter record in the transaction under exactly-once', async () => {
    const input = await kafka.createTopic('dlq-txn-input', 1)
    const output = await kafka.createTopic('dlq-txn-output', 1)
    const dlq = await kafka.createTopic('dlq-txn', 1)
    const applicationId = uniqueName('dlq-txn')
    const app = await errorHandlersApp({
      applicationId,
      brokers,
      input,
      output,
      guarantee: 'exactly-once',
      workers: 0,
      deadLetter: dlq,
    })
    await app.start()

    // In one batch: a malformed record goes to the dead-letter topic, then a code bug (which skips
    // the handlers) stops the app and aborts the batch's transaction.
    await kafka.produce(input, [
      { key: 'bad', value: 'not json', timestamp: now },
      { key: 'bug', value: value('x', { async: true }), timestamp: now + 1 },
    ])
    await eventually(() => app.state === 'error', { message: 'the error state' })
    await expect(app.stop()).rejects.toThrow(AsyncNotAllowedError)

    // The dead-letter record was written, but only in the aborted transaction.
    const uncommitted = await kafka.consume(dlq, 1, { uncommitted: true })
    expect(uncommitted.map(({ key }) => key)).toEqual(['bad'])
    expect(await kafka.consume(dlq, 0, { settleMs: 2_000 })).toEqual([])
    expect(await committedTotal(applicationId, input)).toBe(0n)
  })

  it('always stops on a produce error, even with error handlers', async () => {
    const input = await kafka.createTopic('produce-error-input', 1)
    // The broker rejects record batches over 100 bytes.
    const output = await kafka.createTopic('produce-error-output', 1, {
      'max.message.bytes': '100',
    })
    const dlq = await kafka.createTopic('produce-error-dlq', 1)
    const app = await errorHandlersApp({
      applicationId: uniqueName('produce-error'),
      brokers,
      input,
      output,
      guarantee: 'at-least-once',
      workers: 0,
      deadLetter: dlq,
    })
    await app.start()

    await kafka.produce(input, [{ key: 'k', value: value('x'.repeat(500)), timestamp: now }])
    await eventually(() => app.state === 'error', { message: 'the error state' })
    await expect(app.stop()).rejects.toThrow()
    expect(await kafka.consume(dlq, 0)).toEqual([])
  })

  it('runs the handlers in worker threads, and keeps error classes across threads', async () => {
    const input = await kafka.createTopic('threads-input', 1)
    const output = await kafka.createTopic('threads-output', 1)
    const dlq = await kafka.createTopic('threads-dlq', 1)
    const app = await errorHandlersApp({
      applicationId: uniqueName('dlq-threads'),
      brokers,
      input,
      output,
      guarantee: 'exactly-once',
      workers: 1,
      deadLetter: dlq,
    })
    await app.start()

    await kafka.produce(input, [{ key: 'bad', value: 'not json', timestamp: now }])
    expect((await kafka.consumeWithHeaders(dlq, 1))[0]).toMatchObject({
      key: 'bad',
      headers: { 'kstreams.error.name': 'DeserializationError' },
    })

    await kafka.produce(input, [{ key: 'bug', value: value('x', { async: true }), timestamp: now }])
    await eventually(() => app.state === 'error', { message: 'the error state' })
    const error: unknown = await app.stop().then(
      () => undefined,
      (e: unknown) => e,
    )
    expect(error).toBeInstanceOf(AsyncNotAllowedError)
    expect(error).toMatchObject({ name: 'AsyncNotAllowedError', message: /filter\(\)/ })
    expect((error as Error).stack).toMatch(/AsyncNotAllowedError/)
  })

  it('keeps a DeserializationError and its fields across threads without handlers', async () => {
    const input = await kafka.createTopic('threads-fail-input', 1)
    const output = await kafka.createTopic('threads-fail-output', 1)
    const app = await errorHandlersApp({
      applicationId: uniqueName('threads-fail'),
      brokers,
      input,
      output,
      guarantee: 'at-least-once',
      workers: 1,
    })
    await app.start()

    await kafka.produce(input, [{ key: 'bad', value: 'not json', timestamp: now }])
    await eventually(() => app.state === 'error', { message: 'the error state' })
    const error: unknown = await app.stop().then(
      () => undefined,
      (e: unknown) => e,
    )
    expect(error).toBeInstanceOf(DeserializationError)
    expect(error).toMatchObject({
      name: 'DeserializationError',
      topic: input,
      part: 'value',
      timestamp: now,
      cause: expect.any(SyntaxError),
    })
  })
})
