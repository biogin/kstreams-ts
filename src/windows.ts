// Tumbling windows: their definition, windowed keys, and how windowed keys are written to topics
// and to State Stores.

import type { Serde } from './serde.js'
import type { KeyEncoding } from './state-store.js'
import { decodeKey, encodeKey, isOrderedValue, SerializedKey } from './store-key.js'

/** A length of time: a number of milliseconds, or a number with a unit, e.g. `'10s'` or `'1m'`. */
export type Duration = number | `${number}${'ms' | 's' | 'm' | 'h' | 'd'}`

export interface TumblingOptions {
  /**
   * How far behind Stream Time a record can arrive and still be included in its window. A window
   * closes once Stream Time reaches its end plus the Grace Period; records for it after that are
   * dropped and counted as late. Defaults to 0.
   */
  grace?: Duration
}

/** Fixed-size, non-overlapping windows aligned to the epoch. Create them with {@link tumbling}. */
export interface TumblingWindows {
  readonly sizeMs: number
  readonly graceMs: number
}

/**
 * Tumbling windows of `size`: each record falls into exactly one window, `[start, start + size)`,
 * by its Event Time. The size must be a positive whole number of milliseconds.
 *
 * @example
 * stream.groupByKey().window(tumbling('1m', { grace: '10s' })).count()
 */
export function tumbling(size: Duration, options: TumblingOptions = {}): TumblingWindows {
  const sizeMs = milliseconds(size)
  if (!(Number.isSafeInteger(sizeMs) && sizeMs > 0)) {
    throw new Error(`The window size must be a positive whole number of milliseconds, got ${size}`)
  }
  const graceMs = milliseconds(options.grace ?? 0)
  if (!(Number.isSafeInteger(graceMs) && graceMs >= 0)) {
    throw new Error(
      `The Grace Period must be zero or a positive whole number of milliseconds, got ${options.grace}`,
    )
  }
  return { sizeMs, graceMs }
}

const UNITS: Record<string, number> = { ms: 1, s: 1_000, m: 60_000, h: 3_600_000, d: 86_400_000 }
const DURATION = /^(-?\d+(?:\.\d+)?)(ms|s|m|h|d)$/

function milliseconds(duration: Duration): number {
  if (typeof duration === 'number') return duration
  const match = DURATION.exec(String(duration))
  if (match === null) {
    throw new Error(
      `Invalid duration ${JSON.stringify(duration)}: use milliseconds or a number with ms, s, m, h or d`,
    )
  }
  return Number(match[1]) * (UNITS[match[2] as string] as number)
}

/** A time window: from `start` (included) to `end` (excluded), in milliseconds since the epoch. */
export interface Window {
  start: number
  end: number
}

/** The key of a windowed aggregation's Table: the grouped key and the window it is for. */
export interface Windowed<K> {
  key: K
  window: Window
}

/** Internal: the tumbling window that `timestamp` falls into. */
export function windowOf({ sizeMs }: TumblingWindows, timestamp: number): Window {
  const start = timestamp - (((timestamp % sizeMs) + sizeMs) % sizeMs)
  return { start, end: start + sizeMs }
}

/**
 * Internal: the Serde of windowed keys on topics: the key's Serde bytes followed by the window
 * start as an 8-byte big-endian signed integer. The window end is start plus the window size.
 */
export function windowedSerde<K>(serde: Serde<K>, { sizeMs }: TumblingWindows): Serde<Windowed<K>> {
  return {
    serialize({ key, window }, context) {
      const bytes = serde.serialize(key, context)
      const out = new Uint8Array(bytes.length + 8)
      out.set(bytes)
      new DataView(out.buffer).setBigInt64(bytes.length, BigInt(window.start))
      return out
    },
    deserialize(bytes, context) {
      if (bytes.byteLength < 8) {
        throw new RangeError(
          `Expected at least 8 bytes for a windowed key, got ${bytes.byteLength}`,
        )
      }
      const at = bytes.byteLength - 8
      const start = Number(new DataView(bytes.buffer, bytes.byteOffset).getBigInt64(at))
      const key = serde.deserialize(bytes.subarray(0, at), context)
      return { key, window: { start, end: start + sizeMs } }
    },
  }
}

/**
 * Internal: the store keys of a window store, `[windowStart, key]`. Window start first, so that
 * entries are ordered by time and the windows before a cutoff are one range scan.
 */
export function windowKeyEncoding<K>(
  serde: Serde<K>,
  { sizeMs }: TumblingWindows,
): KeyEncoding<Windowed<K>> {
  return {
    encode({ key, window }, context) {
      const part = isOrderedValue(key) ? key : new SerializedKey(serde.serialize(key, context))
      return encodeKey([window.start, part])
    },
    decode(bytes, context) {
      const [start, part, ...rest] = decodeKey(bytes)
      if (typeof start !== 'number' || part === undefined || rest.length > 0) {
        throw new Error('Invalid store key: expected a window start and a key')
      }
      const key =
        part instanceof SerializedKey ? serde.deserialize(part.bytes, context) : (part as K)
      return { key, window: { start, end: start + sizeMs } }
    },
  }
}
