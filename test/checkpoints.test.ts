import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { CHECKPOINT_FILE, readCheckpoint } from '../src/checkpoint.js'
import {
  createStreamsApp,
  type ProcessorContext,
  type StateStoreSupplier,
  serdes,
  stores,
} from '../src/index.js'
import type { ChangelogReader, ChangelogRecord } from '../src/restore.js'
import { restoreStores } from '../src/restore.js'
import { taskDirectory } from '../src/state-store.js'
import { processorGraphOf } from '../src/streams-app.js'
import { createTask, type Task } from '../src/task.js'

const applicationId = 'checkpoints'
const changelog = `${applicationId}-counts-changelog`
const context = { topic: changelog }
const text = (value: string) => serdes.string.serialize(value, context)

let stateDir: string
let directory: string
const open: Task[] = []

beforeEach(() => {
  stateDir = mkdtempSync(join(tmpdir(), 'kstreams-checkpoints-'))
  directory = taskDirectory(stateDir, applicationId, 0)
})

afterEach(() => {
  for (const task of open.splice(0)) task.close()
  rmSync(stateDir, { recursive: true, force: true })
})

/**
 * A Changelog partition held in memory, as a Kafka partition from offset `start`: reads from an
 * offset outside it read nothing.
 */
function fakeChangelog(start = 0n) {
  const records: ChangelogRecord[] = []
  const reads: (bigint | undefined)[] = []
  let replayed = 0
  const end = () => start + BigInt(records.length)
  const reader: ChangelogReader = {
    async read(_topic, _partition, apply, from) {
      reads.push(from)
      if (from !== undefined && (from < start || from > end())) return undefined
      const chunk = records.slice(Number((from ?? start) - start))
      replayed += chunk.length
      if (chunk.length > 0) apply(chunk)
      return end()
    },
  }
  return {
    records,
    reader,
    /** The `from` of each read, undefined for a full one. */
    reads,
    replayed: () => replayed,
    end,
  }
}

/** Counts its input keys in the store `counts`, which `supplier` opens (SQLite by default). */
function counterTask(
  log: ReturnType<typeof fakeChangelog>,
  supplier: StateStoreSupplier = stores.sqlite(),
): Task {
  const app = createStreamsApp({ applicationId })
  const counts = app.store('counts', { key: serdes.string, value: serdes.number }, { supplier })
  const counter = app.processor('counter', (ctx: ProcessorContext<never, never>) => ({
    process({ key }: { key: string | null }) {
      const store = ctx.store(counts)
      store.put(key ?? '', (store.get(key ?? '') ?? 0) + 1)
    },
  }))
  app.stream('input', { key: serdes.string, value: serdes.string }).process(counter)
  const task = createTask(processorGraphOf(app), () => {}, Date.now, {
    stateDir,
    // Store writes go to the in-memory Changelog, as the broker would acknowledge them.
    changelog: (_topic, { key, value }) => log.records.push({ key, value }),
    restore: (opened) => restoreStores(opened, 0, log.reader),
  })
  open.push(task)
  return task
}

async function pipe(task: Task, ...keys: string[]): Promise<void> {
  for (const key of keys) {
    await task.process('input', { key: text(key), value: text('x'), timestamp: 1 })
  }
}

/** A Task's commit: its Changelog records are acknowledged up to the end of the partition. */
function commit(task: Task, log: ReturnType<typeof fakeChangelog>): void {
  task.checkpoint(new Map([[changelog, log.end()]]))
}

function close(task: Task): void {
  open.splice(open.indexOf(task), 1)
  task.close()
}

/** The count of `key` as the Task's store has it. */
function count(task: Task, key: string): unknown {
  return task.store('counts').get(key)
}

describe('Checkpoints of persistent State Stores', () => {
  it('writes a Checkpoint of the Changelog offset applied with each commit', async () => {
    const log = fakeChangelog()
    const task = counterTask(log)
    await pipe(task, 'a', 'b', 'a')
    commit(task, log)

    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 3n]]))

    await pipe(task, 'c')
    commit(task, log)
    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 4n]]))
  })

  it('writes a Checkpoint once Restore is done, before any record is processed', async () => {
    const log = fakeChangelog()
    log.records.push({ key: text('a'), value: serdes.number.serialize(5, context) })
    const task = counterTask(log)
    await pipe(task)
    await task.fireWallClockTimers()

    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 1n]]))
  })

  it('removes the Checkpoint before the first store write after it (the store is unclean)', async () => {
    const log = fakeChangelog()
    const task = counterTask(log)
    await pipe(task, 'a')
    commit(task, log)
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(true)

    await pipe(task, 'a')
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(false)
  })

  it('restores a restarted Task only from its Checkpoint on, keeping its local store', async () => {
    const log = fakeChangelog()
    const first = counterTask(log)
    await pipe(first, 'a', 'b', 'a')
    commit(first, log)
    close(first)
    // Written after the Checkpoint, e.g. by another instance that took the Task over.
    log.records.push({ key: text('a'), value: serdes.number.serialize(10, context) })
    const replayedBefore = log.replayed()

    const second = counterTask(log)
    await pipe(second, 'a')

    expect(log.reads.at(-1)).toBe(3n)
    expect(log.replayed() - replayedBefore).toBe(1)
    expect(count(second, 'a')).toBe(11)
    expect(count(second, 'b')).toBe(1)
  })

  it('wipes the store and fully restores it when the Checkpoint is missing', async () => {
    const log = fakeChangelog()
    const first = counterTask(log)
    await pipe(first, 'a')
    commit(first, log)
    // Unclean: written, but its Changelog record never acknowledged (e.g. a crash mid-batch).
    await pipe(first, 'ghost')
    log.records.pop()
    close(first)
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(false)

    const second = counterTask(log)
    await pipe(second, 'a')

    expect(log.reads.at(-1)).toBeUndefined()
    expect(count(second, 'ghost')).toBeUndefined()
    expect(count(second, 'a')).toBe(2)
  })

  it.each([
    ['unreadable', 'not json'],
    ['of another format version', JSON.stringify({ version: 999, offsets: { counts: '1' } })],
    ['without the store', JSON.stringify({ version: 1, offsets: {} })],
    ['with a bad offset', JSON.stringify({ version: 1, offsets: { counts: '-1' } })],
  ])('wipes and fully restores when the Checkpoint is %s', async (_, content) => {
    const log = fakeChangelog()
    const first = counterTask(log)
    await pipe(first, 'a')
    commit(first, log)
    close(first)
    writeFileSync(join(directory, CHECKPOINT_FILE), content)

    const second = counterTask(log)
    await pipe(second)
    await second.fireWallClockTimers()

    expect(log.reads.at(-1)).toBeUndefined()
    expect(count(second, 'a')).toBe(1)
  })

  it('wipes and fully restores when the Checkpoint is past the end of the Changelog', async () => {
    const log = fakeChangelog()
    const first = counterTask(log)
    await pipe(first, 'a', 'b')
    commit(first, log)
    close(first)
    // The Changelog was recreated: it now only has one record.
    log.records.splice(0, log.records.length, {
      key: text('z'),
      value: serdes.number.serialize(1, context),
    })

    const second = counterTask(log)
    await pipe(second)
    await second.fireWallClockTimers()

    expect(log.reads.slice(-2)).toEqual([2n, undefined])
    expect(entries(second)).toEqual([['z', 1]])
  })

  it('always fully restores stores that are not persistent, and writes them no Checkpoint', async () => {
    const log = fakeChangelog()
    const first = counterTask(log, stores.inMemory())
    await pipe(first, 'a')
    commit(first, log)
    close(first)
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(false)

    const second = counterTask(log, stores.inMemory())
    await pipe(second, 'a')
    expect(log.reads).toEqual([undefined, undefined])
    expect(count(second, 'a')).toBe(2)
  })

  it('writes no Checkpoint after the Task failed', async () => {
    const log = fakeChangelog()
    const task = counterTask(log)
    await pipe(task, 'a')
    await expect(
      task.process('nowhere', { key: null, value: null, timestamp: 1 }),
    ).rejects.toThrow()

    commit(task, log)
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(false)
  })
})

function entries(t: Task): unknown {
  return t.store('counts').all()
}
