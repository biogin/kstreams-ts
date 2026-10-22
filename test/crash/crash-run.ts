// Shared parts of the crash test suite (ADR-0011): settings, the seeded plan randomness, the input
// that keeps arriving during a run, and the oracle that compares outputs, Changelogs and SQLite
// stores with a reference computed from the input. Used by crash.test.ts (one App Process,
// killed and restarted) and rebalance.test.ts (several App Processes coming and going).

import { type Serde, serdes, stores } from '../../src/index.js'
import { type ChangelogRecord, createChangelogReader } from '../../src/restore.js'
import { keyValueStore, taskDirectory } from '../../src/state-store.js'
import type { AppProcess } from '../integration/harness/app-process.js'
import {
  brokers,
  type createKafka,
  delay,
  type TestRecord,
  uniqueName,
} from '../integration/kafka.js'
import { nextChain } from './crash-app.js'
import { layoutAround, readPartitionLog } from './log-reader.js'

export type Kafka = ReturnType<typeof createKafka>

/** A non-negative integer from the environment variable `name`, or `fallback` when unset. */
export function setting(name: string, fallback: number): number {
  const text = process.env[name]
  if (text === undefined || text.trim() === '') return fallback
  const value = Number(text)
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got "${text}"`)
  }
  return value
}

/** The base seed: KSTREAMS_CRASH_SEED, or random. Iteration i of a test file uses seed + i. */
export const baseSeed = setting('KSTREAMS_CRASH_SEED', Math.floor(Math.random() * 2 ** 31))

/**
 * Whether scenario `name` runs: KSTREAMS_CRASH_SCENARIOS is a comma-separated list of scenario
 * names (default: all).
 */
export function scenarioSelected(name: string): boolean {
  const text = process.env.KSTREAMS_CRASH_SCENARIOS
  if (text === undefined || text.trim() === '') return true
  return text
    .split(',')
    .map((part) => part.trim())
    .includes(name)
}

/** Mulberry32: a small seeded PRNG, so that a failing plan can be replayed. */
export function seededRandom(seed: number): () => number {
  let state = seed >>> 0
  return () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4_294_967_296
  }
}

/** A random integer in [min, max). */
export function between(random: () => number, min: number, max: number): number {
  return min + Math.floor(random() * (max - min))
}

/** A random element of `items` (not empty). */
export function pick<T>(random: () => number, items: readonly T[]): T {
  const item = items[between(random, 0, items.length)]
  if (item === undefined) throw new Error('pick from an empty list')
  return item
}

/** The topics of one iteration: fresh names, deleted when `kafka` closes. */
export interface CrashTopics {
  applicationId: string
  partitions: number
  input: string
  countsOutput: string
  chainsOutput: string
  changelogs: { counts: string; chains: string }
}

export async function createCrashTopics(
  kafka: Kafka,
  prefix: string,
  partitions: number,
): Promise<CrashTopics> {
  const input = await kafka.createTopic(`${prefix}-input`, partitions)
  const countsOutput = await kafka.createTopic(`${prefix}-counts`, partitions)
  const chainsOutput = await kafka.createTopic(`${prefix}-chains`, partitions)
  const applicationId = uniqueName(prefix)
  const changelogs = {
    counts: `${applicationId}-counts-changelog`,
    chains: `${applicationId}-chains-changelog`,
  }
  kafka.deleteOnClose(changelogs.counts)
  kafka.deleteOnClose(changelogs.chains)
  return { applicationId, partitions, input, countsOutput, chainsOutput, changelogs }
}

/**
 * Input that keeps arriving, in small irregular chunks, from `start()` until `finish()`, and for at
 * least `minRecords` records. One producer writing chunk after chunk keeps each key's records in
 * order (a key has one partition).
 */
export class Input {
  readonly sent: TestRecord[] = []
  readonly #kafka: Kafka
  readonly #topic: string
  readonly #data: () => number
  readonly #keys: number
  readonly #minRecords: number
  #producing = false
  #done: Promise<void> = Promise.resolve()

  constructor(kafka: Kafka, topic: string, data: () => number, keys: number, minRecords: number) {
    this.#kafka = kafka
    this.#topic = topic
    this.#data = data
    this.#keys = keys
    this.#minRecords = minRecords
  }

  start(): void {
    this.#producing = true
    this.#done = (async () => {
      while (this.#producing || this.sent.length < this.#minRecords) {
        await this.#produce(between(this.#data, 1, 30))
        await delay(between(this.#data, 0, 60))
      }
    })()
  }

  /** Before `start()`: produces `count` records at once (in chunks of 500). */
  async burst(count: number): Promise<void> {
    for (let left = count; left > 0; left -= 500) await this.#produce(Math.min(left, 500))
  }

  async #produce(count: number): Promise<void> {
    const start = Date.now()
    const chunk = Array.from({ length: count }, (_, i) => ({
      key: `key-${between(this.#data, 0, this.#keys)}`,
      value: String(between(this.#data, 0, 1_000)),
      timestamp: start + i,
    }))
    await this.#kafka.produce(this.#topic, chunk)
    this.sent.push(...chunk)
  }

  /** Stops producing (after `minRecords`) and waits for the last chunk. */
  async finish(): Promise<void> {
    this.#producing = false
    await this.#done
  }

  /**
   * After `finish()`: one record with a key of its own to each of `partitions` partitions, so that
   * every Task gets a record (and opens its State Stores) on whichever process owns it then.
   */
  async tail(partitions: number): Promise<void> {
    const timestamp = Date.now()
    const records = Array.from({ length: partitions }, (_, partition) => ({
      key: `tail-${partition}`,
      value: String(partition),
      timestamp,
      partition,
    }))
    await this.#kafka.produce(this.#topic, records)
    this.sent.push(...records)
  }
}

/** The sum of the committed input offsets of the application. */
export async function consumed(kafka: Kafka, topics: CrashTopics): Promise<bigint> {
  const committed = await kafka.committedOffsets(topics.applicationId, topics.input)
  return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
}

/**
 * Waits until the committed input offsets pass `before`, polling every `pollMs`; fails after
 * `timeoutMs` with the output of `apps`.
 */
export async function progressFrom(
  kafka: Kafka,
  topics: CrashTopics,
  before: bigint,
  apps: () => AppProcess[],
  { pollMs = 20, timeoutMs = 90_000 } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while ((await consumed(kafka, topics)) <= before) {
    if (Date.now() > deadline) {
      throw new Error(`No progress in ${timeoutMs / 1000} s. App output:\n${logs(apps())}`)
    }
    if (pollMs > 0) await delay(pollMs)
  }
}

/** Waits until all of `input` is consumed (committed). */
export async function allConsumed(
  kafka: Kafka,
  topics: CrashTopics,
  input: Input,
  apps: () => AppProcess[],
  timeoutMs = 120_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while ((await consumed(kafka, topics)) !== BigInt(input.sent.length)) {
    if (Date.now() > deadline) {
      throw new Error(
        `Only ${await consumed(kafka, topics)} of ${input.sent.length} input records consumed ` +
          `in ${timeoutMs / 1000} s. App output:\n${logs(apps())}`,
      )
    }
    await delay(100)
  }
}

/** The output of `apps`, for failure messages. */
export function logs(apps: AppProcess[]): string {
  return apps.map((app) => `--- App Process ${app.pid}:\n${app.log()}`).join('\n')
}

/** SIGKILLs `app`; fails if it had already exited by itself. */
export async function expectKilled(app: AppProcess, what: string): Promise<void> {
  const exit = await app.kill()
  if (exit.signal !== 'SIGKILL') {
    throw new Error(`The app exited by itself (${JSON.stringify(exit)}) ${what}:\n${app.log()}`)
  }
}

/** Stops `app` gracefully; fails unless it exits with code 0. */
export async function expectStopped(app: AppProcess, what: string): Promise<void> {
  const exit = await app.stop()
  if (exit.code !== 0 || exit.signal !== null) {
    throw new Error(`The app did not stop cleanly (${JSON.stringify(exit)}) ${what}:\n${app.log()}`)
  }
}

/** The reference: what each key's outputs and final state must be, computed from the input. */
interface Expected {
  counts: number[]
  chains: number[]
}

function reference(sent: TestRecord[]): Map<string, Expected> {
  const expected = new Map<string, Expected>()
  for (const { key, value } of sent) {
    const entry = expected.get(key as string) ?? { counts: [], chains: [] }
    entry.counts.push(entry.counts.length + 1)
    entry.chains.push(nextChain(entry.chains.at(-1) ?? 0, value as string))
    expected.set(key as string, entry)
  }
  return expected
}

/** Describes how `actual` differs from `expected` in a line, or undefined when they are equal. */
function difference(name: string, expected: number[], actual: number[]): string | undefined {
  const first = expected.findIndex((value, i) => actual[i] !== value)
  if (first === -1 && actual.length === expected.length) return undefined
  const at = first === -1 ? expected.length : first
  return (
    `${name}: ${actual.length} records, expected ${expected.length}; first difference at ` +
    `#${at}: got ${actual[at]}, expected ${expected[at]}` +
    (at > 0 ? ` (previous ${actual[at - 1]})` : '')
  )
}

/**
 * Every committed record of `topic`, in offset order per partition, read by the suite's own
 * abort-aware reader (log-reader.ts): not the client's read_committed consumer, which can deliver
 * aborted records (platformatic/kafka#467), and not the engine's. Also reads `topic` with
 * Restore's reader and adds a problem where it disagrees, since Restore relies on it.
 */
async function readCommitted(
  topic: string,
  partitions: number,
  problems: string[],
): Promise<{ key: string; value: Uint8Array | null; partition: number }[]> {
  const records: { key: string; value: Uint8Array | null; partition: number }[] = []
  const restoreReader = createChangelogReader({ brokers, clientId: uniqueName('crash-restore') })
  try {
    for (let partition = 0; partition < partitions; partition++) {
      const log = await readPartitionLog(brokers, uniqueName('crash-log'), topic, partition)
      if (log.unresolved.length > 0) {
        problems.push(`${topic} ${partition}: transactions never ended (${log.unresolved})`)
      }
      for (const { key, value } of log.records) {
        records.push({ key: key?.toString() ?? '', value, partition })
      }

      const restored: ChangelogRecord[] = []
      await restoreReader.read(topic, partition, (chunk) => restored.push(...chunk))
      const same = (a: Uint8Array | null, b: Uint8Array | null) =>
        a === null || b === null ? a === b : Buffer.from(a).equals(Buffer.from(b))
      const at = log.records.findIndex(({ key, value }, i) => {
        const other = restored[i]
        return other === undefined || !same(key, other.key) || !same(value, other.value)
      })
      if (at !== -1 || restored.length !== log.records.length) {
        const offset = log.records[at === -1 ? log.records.length - 1 : at]?.offset ?? 0n
        problems.push(
          `Restore's reader (createChangelogReader) read ${restored.length} committed records ` +
            `of ${topic} ${partition}, the log has ${log.records.length}; first difference at ` +
            `record #${at === -1 ? log.records.length : at} (offset ${offset}). Log around it:\n` +
            layoutAround(log, offset),
        )
      }
    }
  } finally {
    await restoreReader.close()
  }
  return records
}

/** The entries of the SQLite store `name` of a Task, as the app left it on disk. */
function storedEntries<V>(
  stateDir: string,
  applicationId: string,
  task: number,
  name: string,
  value: Serde<V>,
): [string, V][] {
  const directory = taskDirectory(stateDir, applicationId, task)
  const store = stores.sqlite()({ name, applicationId, task, directory })
  try {
    return keyValueStore(store, { key: serdes.string, value }, { topic: name }).all()
  } finally {
    store.close()
  }
}

/**
 * The oracle: compares everything with the reference computed from `sent`, and returns the
 * problems found (none when exact):
 * - outputs: each key's committed counts are exactly 1, 2, ... n and its checksums exactly the
 *   running checksums of its values, in order: nothing missing, no duplicate;
 * - Changelogs: every store write of each key, committed exactly once and in order;
 * - the SQLite stores in `stateDir`, which the last App Process to own every Task left there after
 *   a graceful stop: the exact final count and checksum of each key.
 * Records the number of outputs read in `journal`.
 */
export async function checkResults(
  topics: CrashTopics,
  sent: TestRecord[],
  stateDir: string,
  journal: string[],
): Promise<string[]> {
  const expected = reference(sent)
  const problems: string[] = []

  const outputs = async (topic: string) => {
    const byKey = new Map<string, number[]>()
    for (const { key, value } of await readCommitted(topic, topics.partitions, problems)) {
      const text = value === null ? 'null' : serdes.string.deserialize(value, { topic })
      byKey.set(key, [...(byKey.get(key) ?? []), Number(text)])
    }
    return byKey
  }
  const counts = await outputs(topics.countsOutput)
  const chains = await outputs(topics.chainsOutput)
  const total = (byKey: Map<string, number[]>) =>
    [...byKey.values()].reduce((sum, values) => sum + values.length, 0)
  journal.push(`outputs read: ${total(counts)} counts, ${total(chains)} checksums`)
  for (const key of new Set([...counts.keys(), ...chains.keys()])) {
    if (!expected.has(key)) problems.push(`output for unknown key ${key}`)
  }
  for (const [key, entry] of expected) {
    const count = difference(`counts of ${key}`, entry.counts, counts.get(key) ?? [])
    const chain = difference(`checksums of ${key}`, entry.chains, chains.get(key) ?? [])
    if (count !== undefined) problems.push(count)
    if (chain !== undefined) problems.push(chain)
  }

  for (const name of ['counts', 'chains'] as const) {
    const topic = topics.changelogs[name]
    const byKey = new Map<string, number[]>()
    for (const { key, value } of await readCommitted(topic, topics.partitions, problems)) {
      const number = value === null ? Number.NaN : serdes.number.deserialize(value, { topic })
      byKey.set(key, [...(byKey.get(key) ?? []), number])
    }
    for (const key of byKey.keys()) {
      if (!expected.has(key)) problems.push(`Changelog ${name}: unknown key ${key}`)
    }
    for (const [key, entry] of expected) {
      const found = difference(`Changelog ${name} of ${key}`, entry[name], byKey.get(key) ?? [])
      if (found !== undefined) problems.push(found)
    }
  }

  for (const name of ['counts', 'chains'] as const) {
    const stored = new Map<string, number>()
    for (let task = 0; task < topics.partitions; task++) {
      const entries = storedEntries(stateDir, topics.applicationId, task, name, serdes.number)
      for (const [key, value] of entries) {
        if (stored.has(key)) problems.push(`store ${name}: key ${key} in two Tasks`)
        stored.set(key, value)
      }
    }
    const final = new Map([...expected].map(([key, entry]) => [key, entry[name].at(-1)]))
    for (const key of new Set([...final.keys(), ...stored.keys()])) {
      if (stored.get(key) !== final.get(key)) {
        problems.push(
          `SQLite store ${name}: ${key} is ${stored.get(key)}, expected ${final.get(key)}`,
        )
      }
    }
  }
  return problems
}
