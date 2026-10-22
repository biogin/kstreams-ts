import { existsSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterEach, beforeEach, describe, expect, it } from 'vitest'
import { CHECKPOINT_FILE, CHECKPOINT_VERSION, readCheckpoint } from '../src/checkpoint.js'
import {
  createStreamsApp,
  type ProcessorContext,
  type StateStoreSupplier,
  serdes,
  stores,
} from '../src/index.js'
import type { ChangelogReader, ChangelogRecord } from '../src/restore.js'
import { restoreStores } from '../src/restore.js'
import { openSqliteStore } from '../src/sqlite-store.js'
import { taskDirectory } from '../src/state-store.js'
import { storeKey } from '../src/store-key.js'
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
  const state = { cancelFullReads: false }
  const reader: ChangelogReader = {
    async read(_topic, _partition, apply, from) {
      reads.push(from)
      if (from === undefined && state.cancelFullReads) throw new Error('Restore cancelled')
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
    /** Set `cancelFullReads` to make full reads fail, as a cancelled Restore or a crash. */
    state,
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
  storeTransactions = false,
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
    storeTransactions,
  })
  open.push(task)
  return task
}

async function pipe(task: Task, ...keys: string[]): Promise<void> {
  for (const key of keys) {
    await task.process('input', { key: text(key), value: text('x'), timestamp: 1 }, 0n)
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

  it('fully restores a clean store written in the old key encoding (Checkpoint version 1)', async () => {
    // Before Checkpoint version 2, a key the Serde changes (a Date through JSON) was stored as its
    // Serde bytes; it is now stored as the key the Serde gives back (an ISO string).
    const dateKey = serdes.json<Date>()
    const day = new Date('2026-01-01T00:00:00.000Z')
    const old = openSqliteStore(directory, 'counts')
    old.put(storeKey(day, dateKey, context), serdes.number.serialize(5, context))
    old.close()
    const log = fakeChangelog()
    log.records.push({
      key: dateKey.serialize(day, context),
      value: serdes.number.serialize(5, context),
    })
    // Clean: the store is exactly its Changelog up to the Checkpoint.
    writeFileSync(
      join(directory, CHECKPOINT_FILE),
      JSON.stringify({ version: 1, offsets: { counts: '1' } }),
    )

    const app = createStreamsApp({ applicationId })
    const counts = app.store('counts', { key: dateKey, value: serdes.number })
    const counter = app.processor('counter', (ctx: ProcessorContext<never, never>) => ({
      process({ key }: { key: string | null }) {
        const store = ctx.store(counts)
        const at = new Date(key ?? '')
        store.put(at, (store.get(at) ?? 0) + 1)
      },
    }))
    app.stream('input', { key: serdes.string, value: serdes.string }).process(counter)
    const task = createTask(processorGraphOf(app), () => {}, Date.now, {
      stateDir,
      changelog: (_topic, { key, value }) => log.records.push({ key, value }),
      restore: (opened) => restoreStores(opened, 0, log.reader),
    })
    open.push(task)
    await pipe(task, day.toISOString())

    expect(log.reads[0]).toBeUndefined() // a full Restore
    expect(task.store('counts').all()).toEqual([[day.toISOString(), 6]])
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
    ['without the store', JSON.stringify({ version: CHECKPOINT_VERSION, offsets: {} })],
    [
      'with a bad offset',
      JSON.stringify({ version: CHECKPOINT_VERSION, offsets: { counts: '-1' } }),
    ],
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
      task.process('nowhere', { key: null, value: null, timestamp: 1 }, 0n),
    ).rejects.toThrow()

    commit(task, log)
    expect(existsSync(join(directory, CHECKPOINT_FILE))).toBe(false)
  })
})

function entries(t: Task): unknown {
  return t.store('counts').all()
}

describe('Store transactions under exactly-once (ADR-0002)', () => {
  /** A counter Task whose store writes go into store transactions. */
  const transactional = (log: ReturnType<typeof fakeChangelog>) =>
    counterTask(log, stores.sqlite(), true)

  /** The exactly-once commit: Kafka has committed, then the store transaction and the Checkpoint. */
  function commitAll(task: Task, log: ReturnType<typeof fakeChangelog>): void {
    task.commit()
    commit(task, log)
  }

  it('a cancelled full Restore leaves the store unclean: the next start restores it in full', async () => {
    const log = fakeChangelog()
    const first = transactional(log)
    await pipe(first, 'a', 'b', 'c')
    commitAll(first, log)
    close(first)
    // The Changelog was recreated: its end is now before the Checkpoint's offset 3.
    const one = serdes.number.serialize(1, context)
    log.records.splice(0, log.records.length, { key: text('z'), value: one })
    // The store is wiped for a full Restore, which is then cancelled (a rebalance, a crash).
    log.state.cancelFullReads = true
    const second = transactional(log)
    await expect(second.restored()).rejects.toThrow(/cancelled/)
    close(second)
    log.state.cancelFullReads = false
    // The Changelog grows past the old Checkpoint's offset.
    for (const key of ['y', 'x', 'w']) log.records.push({ key: text(key), value: one })

    const third = transactional(log)
    await third.restored()

    expect(log.reads.at(-1)).toBeUndefined()
    expect(entries(third)).toEqual([
      ['w', 1],
      ['x', 1],
      ['y', 1],
      ['z', 1],
    ])
  })

  it('restores before the first store transaction begins', async () => {
    const log = fakeChangelog()
    log.records.push({ key: text('a'), value: serdes.number.serialize(5, context) })
    const task = transactional(log)
    await task.restored()

    expect(count(task, 'a')).toBe(5)
    await pipe(task, 'a')
    commitAll(task, log)
    expect(count(task, 'a')).toBe(6)
  })

  it('keeps the writes until commit; rollback undoes every write since the last commit', async () => {
    const log = fakeChangelog()
    const task = transactional(log)
    await pipe(task, 'a', 'b')
    commitAll(task, log)

    await pipe(task, 'a', 'c')
    expect(count(task, 'a')).toBe(2)
    task.rollback()

    expect(count(task, 'a')).toBe(1)
    expect(count(task, 'c')).toBeUndefined()
    // The next record begins a new store transaction.
    await pipe(task, 'c')
    commitAll(task, log)
    expect(count(task, 'c')).toBe(1)
  })

  it('keeps the Checkpoint through the writes and their commit, then advances it', async () => {
    const log = fakeChangelog()
    const task = transactional(log)
    await pipe(task, 'a')
    commitAll(task, log)
    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 1n]]))

    await pipe(task, 'a', 'b')
    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 1n]]))
    task.commit()
    // The Changelog already has everything the store commit adds: still a usable Checkpoint.
    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 1n]]))
    commit(task, log)
    expect(readCheckpoint(directory)).toEqual(new Map([['counts', 3n]]))
  })

  it('refuses to write a Checkpoint while a store transaction is open', async () => {
    const log = fakeChangelog()
    const task = transactional(log)
    await pipe(task, 'a')

    expect(() => commit(task, log)).toThrow(/store transaction/)
  })

  it('a crash after the Kafka commit, before the store commit: replays the batch from the Checkpoint', async () => {
    const log = fakeChangelog()
    const first = transactional(log)
    await pipe(first, 'a', 'b')
    commitAll(first, log)
    // Committed to the Changelog (Kafka committed), but the process dies before the store commit.
    await pipe(first, 'a', 'a')
    close(first)

    const second = transactional(log)
    await pipe(second, 'a')
    commitAll(second, log)

    expect(log.reads.at(-1)).toBe(2n)
    expect(count(second, 'a')).toBe(4)
    expect(count(second, 'b')).toBe(1)
  })

  it('a crash after the store commit, before the Checkpoint: replaying the batch again is harmless', async () => {
    const log = fakeChangelog()
    const first = transactional(log)
    await pipe(first, 'a', 'b')
    commitAll(first, log)
    await pipe(first, 'a', 'c')
    first.commit()
    close(first)

    const second = transactional(log)
    await pipe(second, 'a')
    commitAll(second, log)

    expect(log.reads.at(-1)).toBe(2n)
    expect(entries(second)).toEqual([
      ['a', 3],
      ['b', 1],
      ['c', 1],
    ])
  })

  it('an aborted batch: rolled back, and restored from the Checkpoint without a wipe', async () => {
    const log = fakeChangelog()
    const first = transactional(log)
    await pipe(first, 'a')
    commitAll(first, log)
    await pipe(first, 'a', 'ghost')
    // The Kafka transaction aborts: read_committed readers never see its Changelog records.
    log.records.splice(1)
    first.rollback()
    close(first)

    const second = transactional(log)
    await pipe(second, 'a')
    commitAll(second, log)

    expect(log.reads.at(-1)).toBe(1n)
    expect(entries(second)).toEqual([['a', 2]])
  })
})
