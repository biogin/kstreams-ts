// Crash test suite, rebalances (ADR-0011, ticket 20): App Processes and Workers come and go
// mid-run, by graceful stops and by SIGKILLs aimed at rebalances and Restores, while input keeps
// arriving. The oracle is the same as crash.test.ts's (crash-run.ts): outputs, Changelogs and the
// SQLite stores exactly match a reference computed from the input, with nothing missing and no
// duplicate.
//
// Each scenario runs the crash app (crash-app.ts, exactly-once, two SQLite State Stores) in several
// App Processes with the same Application ID. Each process has a state directory, its own or
// shared with others (one host); a killed process may be restarted with its directory (it then
// takes its old App Process ID, so its producers fence its zombie's) or stay dead (its Tasks move
// to other processes, which wait out its open transactions, and take over its Task `.lock` files
// where they share its directory). At the end every process but one, the survivor, stops
// gracefully; the input ends with one record per partition, so the survivor opens every Task;
// once all input is consumed it stops, and the stores in its state directory must be exact.
//
// Scenarios (one iteration each by default):
//   scale-up        processes join one by one (own directory, then a shared one)
//   scale-down      a process killed for good in a shared directory (its Tasks and `.lock` files
//                   move to a process there), then one stopped gracefully
//   kill-rebalance  rounds of a process joining or stopping, with a kill while the group
//                   rebalances; a killed process is restarted or stays dead (its Tasks move)
//   kill-restore    kills while Tasks restore in the background (ticket 19): Restores held open by
//                   a gate (an open, later aborted, transaction on every Changelog partition) so
//                   their held records overflow `commitMaxRecords` and starts get cancelled when
//                   another process takes the Tasks over in the same directory; then a full
//                   Restore of a large Changelog into a fresh directory, killed at a random time
//   workers         2 worker threads per process: kills, a process killed for good in a shared
//                   directory, and a kill while a process joins
//
// Run with `KSTREAMS_KAFKA_BROKERS=localhost:9092 npm run test:crash`. Settings (environment):
//   KSTREAMS_CRASH_SCENARIOS             comma-separated scenarios to run (default all, with
//                                        crash.test.ts's `kill`)
//   KSTREAMS_CRASH_REBALANCE_ITERATIONS  iterations per scenario (default 1)
//   KSTREAMS_CRASH_SEED                  base seed; iteration i of each scenario uses seed + i
//                                        (default: random)
// A failure names its scenario and seed; rerun it alone with KSTREAMS_CRASH_SCENARIOS=<scenario>
// KSTREAMS_CRASH_SEED=<seed> KSTREAMS_CRASH_REBALANCE_ITERATIONS=1. The seed fixes the plan (sizes,
// input values, which process does what and the delays), not the broker's or the app's timing.

import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { Producer } from '@platformatic/kafka'
import { afterEach, beforeEach, expect, it } from 'vitest'
import { type AppProcess, launchAppModule } from '../integration/harness/app-process.js'
import { brokers, createKafka, delay, describeWithKafka, uniqueName } from '../integration/kafka.js'
import type { CrashParams } from './crash-app.js'
import {
  allConsumed,
  baseSeed,
  between,
  type CrashTopics,
  checkResults,
  consumed,
  createCrashTopics,
  expectKilled,
  expectStopped,
  Input,
  type Kafka,
  logs,
  pick,
  progressFrom,
  scenarioSelected,
  seededRandom,
  setting,
} from './crash-run.js'

const crashApp = new URL('./crash-app.ts', import.meta.url)
const crashWorkersApp = new URL('./crash-workers-app.ts', import.meta.url)

const iterations = setting('KSTREAMS_CRASH_REBALANCE_ITERATIONS', 1)

interface Plan {
  partitions: number
  keys: number
  commitIntervalMs: number
  commitMaxRecords: number
  minRecords: number
}

function makePlan(random: () => number): Plan {
  return {
    partitions: between(random, 3, 7),
    keys: between(random, 10, 40),
    commitIntervalMs: pick(random, [20, 50, 100]),
    commitMaxRecords: between(random, 5, 100),
    minRecords: between(random, 1_000, 2_000),
  }
}

/** A running App Process of a scenario: its state directory and the process itself. */
interface Member {
  readonly directory: string
  app: AppProcess
}

/**
 * The App Processes of one iteration, by name, and what is done to them. Every step goes to the
 * journal, which a failure prints.
 */
class Cluster {
  readonly #kafka: Kafka
  readonly #topics: CrashTopics
  readonly #params: CrashParams
  readonly #journal: string[]
  readonly #processes: AppProcess[]
  readonly #directories: string[]
  readonly #input: Input
  readonly #named = new Map<string, string>()
  readonly #members = new Map<string, Member>()
  readonly #started = Date.now()

  constructor(
    kafka: Kafka,
    topics: CrashTopics,
    params: CrashParams,
    input: Input,
    journal: string[],
    processes: AppProcess[],
    directories: string[],
  ) {
    this.#kafka = kafka
    this.#topics = topics
    this.#params = params
    this.#input = input
    this.#journal = journal
    this.#processes = processes
    this.#directories = directories
  }

  /** The state directory called `label`, created on first use. */
  directory(label: string): string {
    let directory = this.#named.get(label)
    if (directory === undefined) {
      directory = mkdtempSync(join(tmpdir(), `kstreams-crash-${label}-`))
      this.#directories.push(directory)
      this.#named.set(label, directory)
    }
    return directory
  }

  note(text: string): void {
    const at = ((Date.now() - this.#started) / 1000).toFixed(1)
    this.#journal.push(
      `${at} s: ${text} (${this.#input.sent.length} records produced, live: ` +
        `${[...this.#members.keys()].join(' ') || 'none'})`,
    )
  }

  /** Starts App Process `name` with state directory `label` and waits until it is running. */
  async launch(name: string, label: string): Promise<void> {
    if (this.#members.has(name)) throw new Error(`${name} is already running`)
    this.note(`launch ${name} (directory ${label})`)
    const directory = this.directory(label)
    const workers = this.#params.workers ?? 0
    const app = await launchAppModule(
      workers > 0 ? crashWorkersApp : crashApp,
      { ...this.#params, stateDir: directory },
      { timeoutMs: 60_000 },
    )
    this.#processes.push(app)
    this.#members.set(name, { directory, app })
  }

  /** SIGKILLs `name`; restarts it with the same state directory when `restart`. */
  async kill(name: string, { restart }: { restart: boolean }): Promise<void> {
    const member = this.#member(name)
    this.note(`kill ${name}${restart ? ', restart it' : ' for good'}`)
    this.#members.delete(name)
    await expectKilled(member.app, `before kill of ${name}`)
    if (restart) await this.launch(name, this.#label(member.directory))
  }

  /** Stops `name` gracefully. */
  async stop(name: string): Promise<void> {
    const member = this.#member(name)
    this.note(`stop ${name}`)
    this.#members.delete(name)
    await expectStopped(member.app, `when stopping ${name}`)
  }

  /** The names of the running App Processes. */
  live(): string[] {
    return [...this.#members.keys()]
  }

  apps(): AppProcess[] {
    return this.#processes
  }

  /** Waits until the committed input offsets advance. */
  async progress(): Promise<void> {
    const before = await consumed(this.#kafka, this.#topics)
    await progressFrom(this.#kafka, this.#topics, before, () => this.apps())
  }

  /**
   * Ends the run: stops every process but `survivor` gracefully, ends the input with one record
   * per partition, waits until it is all consumed and stops `survivor`. Returns its state
   * directory, where every Task's stores now are.
   */
  async finish(survivor: string): Promise<string> {
    const { directory } = this.#member(survivor)
    await this.#input.finish()
    for (const name of this.live()) {
      if (name !== survivor) await this.stop(name)
    }
    await this.#input.tail(this.#topics.partitions)
    this.note(`input ended, survivor ${survivor}`)
    await allConsumed(this.#kafka, this.#topics, this.#input, () => this.apps())
    // A graceful stop commits the last batch and checkpoints the stores.
    await this.stop(survivor)
    return directory
  }

  #member(name: string): Member {
    const member = this.#members.get(name)
    if (member === undefined) {
      throw new Error(`${name} is not running. App output:\n${logs(this.#processes)}`)
    }
    return member
  }

  #label(directory: string): string {
    for (const [label, path] of this.#named) if (path === directory) return label
    throw new Error(`unknown state directory ${directory}`)
  }
}

/** An open transaction of another producer on a Changelog: Restores wait until it ends. */
interface Gate {
  abort(): Promise<void>
}

/** What a scenario gets: the cluster, its plan randomness and the topics. */
interface Scenario {
  cluster: Cluster
  random: () => number
  topics: CrashTopics
  input: Input
  /** Opens a gate: a transaction left open on every partition of `topic`, aborted by `abort()`. */
  openGate(topic: string): Promise<Gate>
}

/** Runs the scenario's steps and returns the survivor's name. */
type ScenarioSteps = (scenario: Scenario) => Promise<string>

interface ScenarioDefinition {
  steps: ScenarioSteps
  workers?: number
  /** Records produced in one burst before the first process starts. */
  preload?: number
}

const scenarios: Record<string, ScenarioDefinition> = {
  'scale-up': {
    async steps({ cluster, random }) {
      await cluster.launch('A', 'a')
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      await cluster.launch('B', 'b')
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      await cluster.launch('C', 'a')
      await cluster.progress()
      return pick(random, cluster.live())
    },
  },

  'scale-down': {
    async steps({ cluster, random }) {
      await cluster.launch('A', 'a')
      await cluster.launch('B', 'b')
      await cluster.launch('C', 'a')
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      // C's Tasks move to A (C's `.lock` files there are stale) or B; both wait out C's open
      // transactions, since nobody takes C's App Process ID.
      await cluster.kill('C', { restart: false })
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      await cluster.stop(pick(random, ['A', 'B']))
      await cluster.progress()
      return pick(random, cluster.live())
    },
  },

  'kill-rebalance': {
    async steps({ cluster, random }) {
      await cluster.launch('A', 'a')
      await cluster.launch('B', 'b')
      await cluster.progress()
      let next = 0
      for (let round = 0; round < 3; round++) {
        const live = cluster.live()
        // A rebalance: a process joins (when there are few) or stops gracefully, in the background.
        let rebalancing: Promise<void>
        let changed: string
        if (live.length <= 2) {
          changed = `P${next++}`
          rebalancing = cluster.launch(changed, pick(random, ['a', 'b', `fresh-${round}`]))
        } else {
          changed = pick(random, live)
          rebalancing = cluster.stop(changed)
        }
        await delay(between(random, 0, 1_500))
        const victims = cluster.live().filter((name) => name !== changed)
        const victim = pick(random, victims)
        // Keep at least one other process running when the victim stays dead.
        const restart = victims.length < 2 || random() < 0.5
        const killing = cluster.kill(victim, { restart })
        await Promise.all([rebalancing, killing])
        await cluster.progress()
      }
      return pick(random, cluster.live())
    },
  },

  'kill-restore': {
    preload: 6_000,
    async steps({ cluster, random, topics, openGate }) {
      await cluster.launch('A', 'a')
      await cluster.progress()
      await cluster.progress()

      // Round 1: every Restore from now on waits for the gate, holding its Task's records.
      const gate = await openGate(topics.changelogs.counts)
      cluster.note('gate opened on every counts Changelog partition')
      await cluster.launch('B', 'b')
      await delay(between(random, 0, 1_000))
      const action = pick(random, ['kill-restoring', 'cancel-start', 'kill-owner'] as const)
      cluster.note(`round 1: ${action}`)
      if (action === 'kill-restoring') {
        await cluster.kill('B', { restart: true })
      } else if (action === 'cancel-start') {
        // C takes some of B's starting Tasks over in B's directory: B cancels their starts and
        // releases their locks, C waits for the locks; a kill comes meanwhile.
        const joining = cluster.launch('C', 'b')
        await delay(between(random, 0, 1_500))
        const killing = cluster.kill(pick(random, ['B', 'A']), { restart: true })
        await Promise.all([joining, killing])
      } else {
        await cluster.kill('A', { restart: true })
      }
      await delay(between(random, 500, 3_000))
      await gate.abort()
      cluster.note('gate aborted')
      await cluster.progress()

      // Round 2: a full Restore of the large Changelogs into a fresh directory, killed at a random
      // time, mostly while it restores.
      await cluster.launch('D', 'd')
      await delay(between(random, 0, 600))
      await cluster.kill('D', { restart: random() < 0.5 })
      await cluster.progress()
      return pick(random, cluster.live())
    },
  },

  workers: {
    workers: 2,
    async steps({ cluster, random }) {
      await cluster.launch('A', 'a')
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      await cluster.kill('A', { restart: true })
      await cluster.progress()
      await cluster.launch('B', 'a')
      await cluster.progress()
      await delay(between(random, 0, 1_000))
      // B's Tasks move to A's Workers, which take over B's stale `.lock` files.
      await cluster.kill('B', { restart: false })
      await cluster.progress()
      const joining = cluster.launch('C', 'c')
      await delay(between(random, 0, 1_500))
      await Promise.all([joining, cluster.kill('A', { restart: true })])
      await cluster.progress()
      return pick(random, cluster.live())
    },
  },
}

describeWithKafka('crash suite: exactly-once across rebalances (SQLite, several processes)', () => {
  let kafka: Kafka
  const processes: AppProcess[] = []
  const directories: string[] = []
  const producers: Producer[] = []

  beforeEach(() => {
    kafka = createKafka()
  })

  afterEach(async () => {
    await Promise.allSettled(processes.splice(0).map((app) => app.kill()))
    await Promise.allSettled(producers.splice(0).map((producer) => producer.close(true)))
    await kafka.close()
    for (const directory of directories.splice(0)) rmSync(directory, { recursive: true })
  })

  async function openGate(topic: string, partitions: number): Promise<Gate> {
    const gate = new Producer({
      clientId: uniqueName('crash-gate'),
      bootstrapBrokers: brokers,
      idempotent: true,
      transactionalId: uniqueName('crash-gate'),
      producerId: -1n,
      producerEpoch: -1,
      timeout: 120_000,
    })
    producers.push(gate)
    const transaction = await gate.beginTransaction()
    await transaction.send({
      messages: Array.from({ length: partitions }, (_, partition) => ({
        topic,
        key: Buffer.from('gate'),
        value: Buffer.from('gate'),
        partition,
      })),
    })
    return transaction
  }

  async function runIteration(
    definition: ScenarioDefinition,
    seed: number,
    journal: string[],
  ): Promise<void> {
    const random = seededRandom(seed)
    const plan = makePlan(random)
    const data = seededRandom(seed ^ 0x9e3779b9)
    journal.push(`plan: ${JSON.stringify(plan)}`)

    const topics = await createCrashTopics(kafka, 'crash-rebalance', plan.partitions)
    const params: CrashParams = {
      applicationId: topics.applicationId,
      brokers,
      input: topics.input,
      countsOutput: topics.countsOutput,
      chainsOutput: topics.chainsOutput,
      stateDir: '',
      commitIntervalMs: plan.commitIntervalMs,
      commitMaxRecords: plan.commitMaxRecords,
      workers: definition.workers ?? 0,
    }
    const input = new Input(kafka, topics.input, data, plan.keys, plan.minRecords)
    const cluster = new Cluster(kafka, topics, params, input, journal, processes, directories)
    if (definition.preload !== undefined) await input.burst(definition.preload)
    input.start()

    let stateDir: string
    try {
      const survivor = await definition.steps({
        cluster,
        random,
        topics,
        input,
        openGate: (topic) => openGate(topic, plan.partitions),
      })
      stateDir = await cluster.finish(survivor)
    } finally {
      await input.finish()
    }
    journal.push(`${input.sent.length} records`)

    expect(await checkResults(topics, input.sent, stateDir, journal)).toEqual([])
  }

  const selected = Object.entries(scenarios).filter(([name]) => scenarioSelected(name))
  if (selected.length === 0 || iterations === 0) it.skip('no rebalance scenario selected', () => {})
  for (const [name, definition] of selected) {
    for (let iteration = 1; iteration <= iterations; iteration++) {
      const seed = (baseSeed + iteration - 1) >>> 0
      it(`${name} ${iteration}/${iterations}: seed ${seed}`, async () => {
        const journal: string[] = []
        const started = Date.now()
        try {
          await runIteration(definition, seed, journal)
        } catch (error) {
          const message = error instanceof Error ? error.message : String(error)
          throw new Error(
            `Crash suite scenario ${name} failed (seed ${seed}). Rerun it with ` +
              `KSTREAMS_CRASH_SCENARIOS=${name} KSTREAMS_CRASH_SEED=${seed} ` +
              `KSTREAMS_CRASH_REBALANCE_ITERATIONS=1.\n${journal.join('\n')}\n${message}`,
            { cause: error },
          )
        }
        console.log(
          `crash suite scenario ${name} (seed ${seed}) passed in ` +
            `${Math.round((Date.now() - started) / 1000)} s:\n  ${journal.join('\n  ')}`,
        )
      }, 480_000)
    }
  }
})
