// Crash test suite v1 (ADR-0011, spec seam 2): the evidence behind the exactly-once claim.
//
// Each iteration runs the crash app (test/crash/crash-app.ts: `count()` and `aggregate()` on SQLite
// State Stores, exactly-once, several partitions) as one Worker in a child process, and SIGKILLs it
// from outside at randomized times while input keeps arriving, restarting it after each kill. Then
// it compares everything against a reference computed from the input:
//
// - outputs (read with an abort-aware reader): each key's counts are exactly 1, 2, ... n and its
//   checksums exactly the running checksums of its values, in order: nothing missing, no duplicate;
// - all input consumed: the committed offsets cover every input record;
// - the SQLite stores on disk, after a graceful stop, and the last Changelog value of each key hold
//   the exact final count and checksum.
//
// There are no fault-injection hooks, so the narrow windows (between the Kafka commit, the SQLite
// commit and the Checkpoint) are covered statistically: batches are short, commits frequent, and
// kill times are biased toward commits (right after committed offsets advance, around multiples
// of the commit interval, during startup and Restore, and uniformly at random).
//
// Run with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:crash`. Settings (environment):
//   KSTREAMS_CRASH_ITERATIONS  iterations, each with fresh topics and Application ID (default 4)
//   KSTREAMS_CRASH_KILLS       SIGKILLs per iteration (default 5)
//   KSTREAMS_CRASH_SEED        base seed; iteration i uses seed + i (default: random)
// The seed fixes the plan (topology sizes, input values, kill strategies and delays), not the
// broker's or the app's timing. A failure names its seed; rerun that iteration alone with
// KSTREAMS_CRASH_SEED=<seed> KSTREAMS_CRASH_ITERATIONS=1.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { type Serde, serdes, stores } from '../../src/index.js'
import { type ChangelogRecord, createChangelogReader } from '../../src/restore.js'
import { keyValueStore, taskDirectory } from '../../src/state-store.js'
import { type AppProcess, launchAppModule } from '../integration/harness/app-process.js'
import {
  brokers,
  createKafka,
  delay,
  describeWithKafka,
  type TestRecord,
  uniqueName,
} from '../integration/kafka.js'
import { type CrashParams, nextChain } from './crash-app.js'
import { layoutAround, readPartitionLog } from './log-reader.js'

const crashApp = new URL('./crash-app.ts', import.meta.url)

function setting(name: string, fallback: number): number {
  const text = process.env[name]
  if (text === undefined || text.trim() === '') return fallback
  const value = Number(text)
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer, got "${text}"`)
  }
  return value
}

const iterations = setting('KSTREAMS_CRASH_ITERATIONS', 4)
const killsPerIteration = setting('KSTREAMS_CRASH_KILLS', 5)
const baseSeed = setting('KSTREAMS_CRASH_SEED', Math.floor(Math.random() * 2 ** 31))

/** Mulberry32: a small seeded PRNG, so that a failing plan can be replayed. */
function seededRandom(seed: number): () => number {
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
function between(random: () => number, min: number, max: number): number {
  return min + Math.floor(random() * (max - min))
}

/**
 * When a kill comes, relative to the incarnation it kills:
 * - `startup`: soon after it is running, before or during Restore and its first commits;
 * - `after-commit`: as soon as its committed offsets are seen to advance (polled back to back),
 *   aiming at the SQLite commit and the Checkpoint that follow the Kafka commit;
 * - `boundary`: a few commit intervals (with jitter) after a commit was seen;
 * - `uniform`: at a uniformly random time after a commit was seen.
 */
type KillStrategy = 'startup' | 'after-commit' | 'boundary' | 'uniform'

interface Kill {
  strategy: KillStrategy
  /** Delay before the kill, from the start of the strategy's wait point. */
  delayMs: number
}

interface Plan {
  partitions: number
  keys: number
  commitIntervalMs: number
  commitMaxRecords: number
  /** At least this many input records, produced until the last kill has happened. */
  minRecords: number
  kills: Kill[]
}

function makePlan(random: () => number): Plan {
  const commitIntervalMs = [20, 50, 100][between(random, 0, 3)] ?? 50
  const strategies: KillStrategy[] = ['startup', 'after-commit', 'boundary', 'uniform']
  const kills = Array.from({ length: killsPerIteration }, (): Kill => {
    const strategy = strategies[between(random, 0, strategies.length)] ?? 'uniform'
    if (strategy === 'startup') return { strategy, delayMs: between(random, 0, 400) }
    if (strategy === 'after-commit') return { strategy, delayMs: 0 }
    if (strategy === 'boundary') {
      const intervals = between(random, 1, 8)
      const jitter = between(random, -3, 4)
      return { strategy, delayMs: Math.max(0, intervals * commitIntervalMs + jitter) }
    }
    return { strategy, delayMs: between(random, 0, 1_000) }
  })
  return {
    partitions: between(random, 2, 5),
    keys: between(random, 5, 30),
    commitIntervalMs,
    commitMaxRecords: between(random, 5, 100),
    minRecords: between(random, 1_000, 2_000),
    kills,
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

describeWithKafka('crash suite: exactly-once under SIGKILL (SQLite, one Worker)', () => {
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

  async function runIteration(seed: number, journal: string[]): Promise<void> {
    const plan = makePlan(seededRandom(seed))
    const data = seededRandom(seed ^ 0x9e3779b9)
    journal.push(`plan: ${JSON.stringify({ ...plan, kills: undefined })}`)

    const input = await kafka.createTopic('crash-input', plan.partitions)
    const countsOutput = await kafka.createTopic('crash-counts', plan.partitions)
    const chainsOutput = await kafka.createTopic('crash-chains', plan.partitions)
    const applicationId = uniqueName('crash')
    const changelogs = {
      counts: `${applicationId}-counts-changelog`,
      chains: `${applicationId}-chains-changelog`,
    }
    kafka.deleteOnClose(changelogs.counts)
    kafka.deleteOnClose(changelogs.chains)
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-crash-'))
    directories.push(stateDir)
    const params: CrashParams = {
      applicationId,
      brokers,
      input,
      countsOutput,
      chainsOutput,
      stateDir,
      commitIntervalMs: plan.commitIntervalMs,
      commitMaxRecords: plan.commitMaxRecords,
    }

    // Input keeps arriving, in small irregular chunks, until every kill has happened. One producer
    // writing chunk after chunk keeps each key's records in order (a key has one partition).
    const sent: TestRecord[] = []
    const start = Date.now()
    let killing = true
    const producing = (async () => {
      while (killing || sent.length < plan.minRecords) {
        const chunk = Array.from({ length: between(data, 1, 30) }, (_, i) => {
          const n = sent.length + i
          return {
            key: `key-${between(data, 0, plan.keys)}`,
            value: String(between(data, 0, 1_000)),
            timestamp: start + n,
          }
        })
        await kafka.produce(input, chunk)
        sent.push(...chunk)
        await delay(between(data, 0, 60))
      }
    })()

    const consumed = async () => {
      const committed = await kafka.committedOffsets(applicationId, input)
      return [...committed.values()].reduce((sum, offset) => sum + offset, 0n)
    }
    /** Waits until the committed offsets pass `before`, polling every `pollMs`. */
    const progressFrom = async (before: bigint, app: AppProcess, pollMs: number) => {
      const deadline = Date.now() + 90_000
      while ((await consumed()) <= before) {
        if (Date.now() > deadline) {
          throw new Error(`No progress in 90 s. App output:\n${app.log()}`)
        }
        if (pollMs > 0) await delay(pollMs)
      }
    }
    const launch = async () => {
      const app = await launchAppModule(crashApp, params, { timeoutMs: 60_000 })
      processes.push(app)
      return app
    }
    const expectKilled = async (app: AppProcess, what: string) => {
      const exit = await app.kill()
      if (exit.signal !== 'SIGKILL') {
        throw new Error(`The app exited by itself (${JSON.stringify(exit)}) ${what}:\n${app.log()}`)
      }
    }

    try {
      let app = await launch()
      for (const [n, { strategy, delayMs }] of plan.kills.entries()) {
        if (strategy !== 'startup') {
          const before = await consumed()
          await progressFrom(before, app, strategy === 'after-commit' ? 0 : 20)
        }
        await delay(delayMs)
        journal.push(
          `kill ${n + 1}: ${strategy} +${delayMs} ms, ${sent.length} records produced, ` +
            `${await consumed()} consumed`,
        )
        await expectKilled(app, `before kill ${n + 1}`)
        app = await launch()
      }
      killing = false
      await producing

      const deadline = Date.now() + 120_000
      while ((await consumed()) !== BigInt(sent.length)) {
        if (Date.now() > deadline) {
          throw new Error(
            `Only ${await consumed()} of ${sent.length} input records consumed in 120 s. ` +
              `App output:\n${app.log()}`,
          )
        }
        await delay(100)
      }
      // A graceful stop commits the last batch and checkpoints the stores.
      expect(await app.stop(), app.log()).toEqual({ code: 0, signal: null })
    } finally {
      killing = false
      await producing
    }
    journal.push(`${sent.length} records`)

    const expected = reference(sent)
    const problems: string[] = []

    // Outputs: exactly the reference sequences.
    const outputs = async (topic: string) => {
      const byKey = new Map<string, number[]>()
      for (const { key, value } of await readCommitted(topic, plan.partitions, problems)) {
        const text = value === null ? 'null' : serdes.string.deserialize(value, { topic })
        byKey.set(key, [...(byKey.get(key) ?? []), Number(text)])
      }
      return byKey
    }
    const counts = await outputs(countsOutput)
    const chains = await outputs(chainsOutput)
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

    // The SQLite stores on disk and the Changelogs: the final values.
    const finals = (pick: (entry: Expected) => number[]) =>
      new Map([...expected].map(([key, entry]) => [key, pick(entry).at(-1)]))
    const stored = { counts: new Map<string, number>(), chains: new Map<string, number>() }
    for (let task = 0; task < plan.partitions; task++) {
      for (const name of ['counts', 'chains'] as const) {
        for (const [key, value] of storedEntries(
          stateDir,
          applicationId,
          task,
          name,
          serdes.number,
        )) {
          if (stored[name].has(key)) problems.push(`store ${name}: key ${key} in two Tasks`)
          stored[name].set(key, value)
        }
      }
    }
    // The Changelogs: every store write of each key, committed exactly once and in order.
    for (const name of ['counts', 'chains'] as const) {
      const topic = changelogs[name]
      const byKey = new Map<string, number[]>()
      for (const { key, value } of await readCommitted(topic, plan.partitions, problems)) {
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
      const final = finals((entry) => entry[name])
      for (const key of new Set([...final.keys(), ...stored[name].keys()])) {
        if (stored[name].get(key) !== final.get(key)) {
          problems.push(
            `SQLite store ${name}: ${key} is ${stored[name].get(key)}, expected ${final.get(key)}`,
          )
        }
      }
    }

    expect(problems).toEqual([])
  }

  for (let iteration = 1; iteration <= iterations; iteration++) {
    const seed = (baseSeed + iteration - 1) >>> 0
    it(
      `iteration ${iteration}/${iterations}: seed ${seed}, ${killsPerIteration} SIGKILLs`,
      async () => {
        const journal: string[] = []
        const started = Date.now()
        try {
          await runIteration(seed, journal)
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)
          throw new Error(
            `Crash suite failed in iteration ${iteration} (seed ${seed}). Rerun it with ` +
              `KSTREAMS_CRASH_SEED=${seed} KSTREAMS_CRASH_ITERATIONS=1 ` +
              `KSTREAMS_CRASH_KILLS=${killsPerIteration}.\n${journal.join('\n')}\n${message}`,
            { cause: error },
          )
        }
        console.log(
          `crash suite iteration ${iteration} (seed ${seed}) passed in ` +
            `${Math.round((Date.now() - started) / 1000)} s:\n  ${journal.join('\n  ')}`,
        )
      },
      killsPerIteration * 60_000 + 240_000,
    )
  }
})
