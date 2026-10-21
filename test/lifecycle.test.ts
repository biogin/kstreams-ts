import { describe, expect, expectTypeOf, it } from 'vitest'
import { type ApplicationState, createStreamsApp, serdes } from '../src/index.js'

// Behavior that needs a broker is covered by the integration tests in test/integration/.

describe('Streams App configuration', () => {
  it('defaults to at-least-once with workers: 0', () => {
    expect(() => createStreamsApp({ applicationId: 'defaults' })).not.toThrow()
    expect(() =>
      createStreamsApp({
        applicationId: 'explicit',
        brokers: ['localhost:9092'],
        guarantee: 'at-least-once',
        workers: 0,
        commitIntervalMs: 50,
        commitMaxRecords: 10,
      }),
    ).not.toThrow()
  })

  it('accepts exactly-once', () => {
    expect(() =>
      createStreamsApp({ applicationId: 'eos', guarantee: 'exactly-once' }),
    ).not.toThrow()
  })

  it('rejects an unknown Delivery Guarantee', () => {
    // biome-ignore lint/suspicious/noExplicitAny: simulates an untyped caller
    const guarantee: any = 'at-most-once'
    expect(() => createStreamsApp({ applicationId: 'bad', guarantee })).toThrow(
      /Unknown Delivery Guarantee "at-most-once"/,
    )
  })

  it('rejects workers > 0 as not implemented yet', () => {
    expect(() => createStreamsApp({ applicationId: 'workers', workers: 2 })).toThrow(
      /workers > 0 is not implemented yet/,
    )
  })

  it.each([-1, 1.5, Number.NaN])('rejects workers: %s', (workers) => {
    expect(() => createStreamsApp({ applicationId: 'workers', workers })).toThrow(
      /workers must be a whole number/,
    )
  })

  it.each([
    ['commitIntervalMs', 0],
    ['commitIntervalMs', Number.POSITIVE_INFINITY],
    ['commitMaxRecords', 0],
    ['commitMaxRecords', 2.5],
    ['sessionTimeoutMs', -5],
    ['shutdownTimeoutMs', 0],
    ['shutdownTimeoutMs', Number.NaN],
    ['changelogReplicationFactor', 0],
    ['changelogReplicationFactor', 1.5],
  ])('rejects %s: %s', (option, value) => {
    expect(() => createStreamsApp({ applicationId: 'commit', [option]: value })).toThrow(
      new RegExp(`${option} must be a positive`),
    )
  })

  it('rejects an empty broker list', () => {
    expect(() => createStreamsApp({ applicationId: 'brokers', brokers: [] })).toThrow(
      /brokers must list at least one/,
    )
  })
})

describe('Application State', () => {
  it('starts as created', () => {
    const app = createStreamsApp({ applicationId: 'state' })
    expect(app.state).toBe('created')
    expectTypeOf(app.state).toEqualTypeOf<ApplicationState>()
  })

  it('refuses to start without brokers', async () => {
    const app = createStreamsApp({ applicationId: 'no-brokers' })
    app.stream('input', { key: serdes.string, value: serdes.string }).to('output')
    await expect(app.start()).rejects.toThrow(/no brokers/i)
    expect(app.state).toBe('created')
  })

  it('refuses to start an app that reads no topic', async () => {
    const app = createStreamsApp({ applicationId: 'empty', brokers: ['localhost:9092'] })
    await expect(app.start()).rejects.toThrow(/reads no topic/)
    expect(app.state).toBe('created')
  })

  it('stops an app that never started', async () => {
    const app = createStreamsApp({ applicationId: 'never-started' })
    await app.stop()
    expect(app.state).toBe('stopped')
    await app.stop()
    expect(app.state).toBe('stopped')
  })

  it('cannot start once stopped', async () => {
    const app = createStreamsApp({ applicationId: 'stopped', brokers: ['localhost:9092'] })
    app.stream('input', { key: serdes.string, value: serdes.string }).to('output')
    await app.stop()
    await expect(app.start()).rejects.toThrow(/cannot start.*stopped/i)
  })
})
