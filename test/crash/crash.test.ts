// Crash test suite v1 (ADR-0011, spec seam 2): the evidence behind the exactly-once claim.
//
// Each iteration runs the crash app (test/crash/crash-app.ts: `count()` and `aggregate()` on SQLite
// State Stores, exactly-once, several partitions) as one Worker in a child process, and SIGKILLs it
// from outside at randomized times while input keeps arriving, restarting it after each kill. Then
// it compares everything against a reference computed from the input (crash-run.ts):
//
// - outputs (read with an abort-aware reader): each key's counts are exactly 1, 2, ... n and its
//   checksums exactly the running checksums of its values, in order: nothing missing, no duplicate;
// - all input consumed: the committed offsets cover every input record;
// - the SQLite stores on disk, after a graceful stop, and the Changelogs of each key hold the
//   exact counts and checksums.
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
//   KSTREAMS_CRASH_SCENARIOS   comma-separated scenarios to run (default all); this file's is
//                              `kill`, rebalance.test.ts has the others
// The seed fixes the plan (topology sizes, input values, kill strategies and delays), not the
// broker's or the app's timing. A failure names its seed; rerun that iteration alone with
// KSTREAMS_CRASH_SEED=<seed> KSTREAMS_CRASH_ITERATIONS=1.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { type AppProcess, launchAppModule } from '../integration/harness/app-process.js'
import { brokers, createKafka, delay, describeWithKafka } from '../integration/kafka.js'
import type { CrashParams } from './crash-app.js'
import {
  allConsumed,
  baseSeed,
  between,
  checkResults,
  consumed,
  createCrashTopics,
  expectKilled,
  expectStopped,
  Input,
  type Kafka,
  progressFrom,
  scenarioSelected,
  seededRandom,
  setting,
} from './crash-run.js'

const crashApp = new URL('./crash-app.ts', import.meta.url)

const iterations = scenarioSelected('kill') ? setting('KSTREAMS_CRASH_ITERATIONS', 4) : 0
const killsPerIteration = setting('KSTREAMS_CRASH_KILLS', 5)

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

describeWithKafka('crash suite: exactly-once under SIGKILL (SQLite, one Worker)', () => {
  let kafka: Kafka
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

  async function runIteration(seed: number, journal: string[]): Promise<void> {
    const plan = makePlan(seededRandom(seed))
    const data = seededRandom(seed ^ 0x9e3779b9)
    journal.push(`plan: ${JSON.stringify({ ...plan, kills: undefined })}`)

    const topics = await createCrashTopics(kafka, 'crash', plan.partitions)
    const stateDir = mkdtempSync(join(tmpdir(), 'kstreams-crash-'))
    directories.push(stateDir)
    const params: CrashParams = {
      applicationId: topics.applicationId,
      brokers,
      input: topics.input,
      countsOutput: topics.countsOutput,
      chainsOutput: topics.chainsOutput,
      stateDir,
      commitIntervalMs: plan.commitIntervalMs,
      commitMaxRecords: plan.commitMaxRecords,
    }

    // Input keeps arriving until every kill has happened.
    const input = new Input(kafka, topics.input, data, plan.keys, plan.minRecords)
    input.start()
    const launch = async () => {
      const app = await launchAppModule(crashApp, params, { timeoutMs: 60_000 })
      processes.push(app)
      return app
    }

    try {
      let app = await launch()
      for (const [n, { strategy, delayMs }] of plan.kills.entries()) {
        if (strategy !== 'startup') {
          const before = await consumed(kafka, topics)
          await progressFrom(kafka, topics, before, () => [app], {
            pollMs: strategy === 'after-commit' ? 0 : 20,
          })
        }
        await delay(delayMs)
        journal.push(
          `kill ${n + 1}: ${strategy} +${delayMs} ms, ${input.sent.length} records produced, ` +
            `${await consumed(kafka, topics)} consumed`,
        )
        await expectKilled(app, `before kill ${n + 1}`)
        app = await launch()
      }
      await input.finish()
      await allConsumed(kafka, topics, input, () => [app])
      // A graceful stop commits the last batch and checkpoints the stores.
      await expectStopped(app, 'at the end')
    } finally {
      await input.finish()
    }
    journal.push(`${input.sent.length} records`)

    expect(await checkResults(topics, input.sent, stateDir, journal)).toEqual([])
  }

  if (iterations === 0) it.skip('scenario kill not selected', () => {})
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
