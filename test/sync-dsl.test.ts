import { describe, expect, expectTypeOf, it } from 'vitest'
import { AsyncNotAllowedError, createStreamsApp, createTestDriver, serdes } from '../src/index.js'

const MESSAGE = 'DSL functions must be synchronous; use a Processor for async work'

function stringStream() {
  const app = createStreamsApp({ applicationId: 'sync-dsl' })
  const stream = app.stream('input', { key: serdes.string, value: serdes.string })
  return { app, stream }
}

/** Collects the unhandled rejections reported while `run` executes and one turn of the event loop. */
async function unhandledRejectionsDuring(run: () => void): Promise<unknown[]> {
  const rejections: unknown[] = []
  const listener = (reason: unknown) => rejections.push(reason)
  process.on('unhandledRejection', listener)
  try {
    run()
    await new Promise((resolve) => setTimeout(resolve, 10))
  } finally {
    process.off('unhandledRejection', listener)
  }
  return rejections
}

// Untyped JavaScript, or TypeScript with `any` casts, gets past the type check.
// biome-ignore lint/suspicious/noExplicitAny: simulates a caller without types
type Untyped = any

describe('async functions are rejected when the app is built', () => {
  it.each([
    ['filter', (stream: Untyped) => stream.filter(async () => true)],
    [
      'map',
      (stream: Untyped) => stream.map(async (key: string, value: string) => ({ key, value })),
    ],
    ['peek', (stream: Untyped) => stream.peek(async () => {})],
    [
      'peek',
      (stream: Untyped) =>
        stream.peek(async function* () {
          yield
        }),
    ],
  ])('%s', (operator, define) => {
    const { stream } = stringStream()
    const build = () => define(stream)

    expect(build).toThrow(AsyncNotAllowedError)
    expect(build).toThrow(`${operator}()`)
    expect(build).toThrow(/Use a Processor/)
  })
})

describe('a function returning a Promise stops the app on the first record', () => {
  it.each([
    ['filter', (stream: Untyped) => stream.filter(() => Promise.resolve(true)).to('output')],
    [
      'map',
      (stream: Untyped) =>
        stream
          .map((key: string, value: string) => Promise.resolve({ key, value }))
          .to('output', { key: serdes.string, value: serdes.string }),
    ],
    ['peek', (stream: Untyped) => stream.peek(() => Promise.resolve()).to('output')],
  ])('%s', (operator, define) => {
    const { app, stream } = stringStream()
    define(stream)
    const input = createTestDriver(app).input<string, string>('input')

    const pipe = () => input.pipe('k', 'v', { timestamp: 1 })

    expect(pipe).toThrow(AsyncNotAllowedError)
    expect(pipe).toThrow(`${operator}()`)
  })

  it('catches any thenable, not only native Promises', () => {
    const { app, stream } = stringStream()
    // biome-ignore lint/suspicious/noThenProperty: a thenable that is not a native Promise
    const thenable = { then: (resolve: (value: boolean) => void) => resolve(true) }
    ;(stream as Untyped).filter(() => thenable).to('output')
    const driver = createTestDriver(app)

    expect(() => driver.input('input').pipe('k', 'v')).toThrow(AsyncNotAllowedError)
  })

  it('sends nothing downstream', () => {
    const { app, stream } = stringStream()
    ;(stream as Untyped).filter(() => Promise.resolve(true)).to('output')
    const driver = createTestDriver(app)

    expect(() => driver.input('input').pipe('k', 'v')).toThrow(AsyncNotAllowedError)
    expect(driver.output('output').readAll()).toEqual([])
  })

  it('leaves no unhandled rejection behind when the Promise rejects', async () => {
    const { app, stream } = stringStream()
    ;(stream as Untyped).peek(() => Promise.reject(new Error('lookup failed'))).to('output')
    const driver = createTestDriver(app)

    const rejections = await unhandledRejectionsDuring(() => {
      expect(() => driver.input('input').pipe('k', 'v')).toThrow(AsyncNotAllowedError)
    })

    expect(rejections).toEqual([])
  })
})

describe('AsyncNotAllowedError', () => {
  it('is an Error named AsyncNotAllowedError', () => {
    const { stream } = stringStream()
    let thrown: unknown
    try {
      ;(stream as Untyped).peek(async () => {})
    } catch (error) {
      thrown = error
    }

    expect(thrown).toBeInstanceOf(Error)
    expect(thrown).toMatchObject({ name: 'AsyncNotAllowedError' })
  })
})

// Type-level tests: checked by `npm run typecheck`, not at runtime.
describe('async functions fail to compile', () => {
  it('rejects async callbacks to filter, map and peek', () => {
    const { stream } = stringStream()
    // @ts-expect-error DSL functions must be synchronous
    expect(() => stream.filter(async () => true)).toThrow(AsyncNotAllowedError)
    // @ts-expect-error DSL functions must be synchronous
    expect(() => stream.map(async (key, value) => ({ key, value }))).toThrow(AsyncNotAllowedError)
    // @ts-expect-error DSL functions must be synchronous
    expect(() => stream.peek(async () => {})).toThrow(AsyncNotAllowedError)
    // @ts-expect-error DSL functions must be synchronous
    expect(() => stream.peek(() => Promise.resolve())).not.toThrow()
  })

  it('names the mistake and points to Processors in the error', () => {
    const { stream } = stringStream()
    expectTypeOf<
      ReturnType<Parameters<typeof stream.map<Promise<{ key: string; value: string }>>>[0]>
    >().toHaveProperty(MESSAGE)
    expectTypeOf<ReturnType<Parameters<typeof stream.peek<Promise<void>>>[0]>>().toHaveProperty(
      MESSAGE,
    )
  })

  it('still accepts synchronous callbacks with any return type for peek', () => {
    const { stream } = stringStream()
    expectTypeOf(stream.peek((key) => key.length)).toEqualTypeOf(stream)
  })
})
