// Enforces that functions passed to the DSL are synchronous, at three layers (ADR-0012): their
// types, when the app is built, and per record.

/**
 * A DSL function was async or returned a Promise (or any thenable). Async work belongs in a
 * Processor. This is a code bug, not a bad record, so it skips the error handlers and always stops
 * the application.
 */
export class AsyncNotAllowedError extends Error {
  override readonly name = 'AsyncNotAllowedError'

  /** @param operator the DSL operator the function was passed to, e.g. `filter`. */
  constructor(operator: string, problem: string) {
    super(
      `DSL functions must be synchronous: the function passed to ${operator}() ${problem}. Use a Processor for async work.`,
    )
  }
}

/** Internal: whether `error` must stop the application without going to the error handlers. */
export function skipsErrorHandlers(error: unknown): boolean {
  return error instanceof AsyncNotAllowedError
}

/** What a DSL function returning a Promise is told by the compiler. */
interface DslFunctionsMustBeSynchronous {
  'DSL functions must be synchronous; use a Processor for async work': never
}

/** The return type `R` of a DSL function, or a compile error if `R` is a Promise. */
export type Sync<R> = R extends PromiseLike<unknown> ? DslFunctionsMustBeSynchronous : R

/** The return type of a `map` function: a key and a value, never a Promise. */
export type SyncKeyValue<R> =
  R extends PromiseLike<unknown>
    ? DslFunctionsMustBeSynchronous
    : R extends { key: unknown; value: unknown }
      ? R
      : { key: unknown; value: unknown }

const ASYNC_FUNCTION_TAGS = new Set(['[object AsyncFunction]', '[object AsyncGeneratorFunction]'])

/**
 * Checks a function given to DSL operator `operator` when the app is built, and returns it wrapped
 * so every call checks its result.
 */
export function synchronous<Args extends unknown[], R>(
  operator: string,
  fn: (...args: Args) => R,
): (...args: Args) => R {
  if (ASYNC_FUNCTION_TAGS.has(Object.prototype.toString.call(fn))) {
    throw new AsyncNotAllowedError(operator, 'is declared async')
  }
  return (...args) => {
    const result = fn(...args)
    if (typeof (result as { then?: unknown } | null | undefined)?.then === 'function') {
      // The app stops on the error below; don't also report the abandoned Promise's rejection.
      Promise.resolve(result).catch(() => {})
      throw new AsyncNotAllowedError(operator, 'returned a Promise')
    }
    return result
  }
}
