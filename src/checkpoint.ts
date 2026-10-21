// Internal: Checkpoints, the Changelog offsets a Task's persistent State Stores have applied up
// to, kept in the Task's state directory so that Restore replays only what comes after them
// (ADR-0009).
//
// A Task's Checkpoint is one file, `<stateDir>/<applicationId>/<task>/.checkpoint`:
//
//   {"version":1,"offsets":{"counts":"42"}}
//
// `offsets` maps each persistent store's name to the offset of its Changelog partition that
// Restore starts from: one past the last record the store has applied (a decimal string, since
// offsets are 64-bit). `version` is the on-disk format of the Checkpoint, the store layout and the
// store key encoding (ADR-0015): a file of another version counts as missing.
//
// Clean and unclean: a store is clean only if its contents are exactly its Changelog up to its
// Checkpoint offset. The Checkpoint is written (atomically, after flushing the stores) only once
// the Changelog records of every store write are acknowledged, and it is removed (durably) before
// the first store write that could make a store differ from it: a put or delete outside a store
// transaction, or a store transaction's commit. So a store is unclean, and is wiped and fully
// restored, when the Checkpoint file is missing (never written, or removed and the process
// stopped before the next one was written, e.g. a crash mid-batch), unreadable, of another
// version, doesn't list the store, or has an offset outside the Changelog partition (before its
// start or past its end, e.g. a recreated topic).

import {
  closeSync,
  fsyncSync,
  mkdirSync,
  openSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from 'node:fs'
import { join } from 'node:path'
import type { StateStore } from './state-store.js'

/** The name of a Task's Checkpoint file in its state directory. */
export const CHECKPOINT_FILE = '.checkpoint'
/** The current on-disk format version (ADR-0015). */
export const CHECKPOINT_VERSION = 1

/** Changelog offsets by store name. */
export type CheckpointOffsets = ReadonlyMap<string, bigint>

/**
 * The Checkpoint in `directory`: the offset each store has applied its Changelog up to. Undefined
 * when there is none, it can't be read, or it is of another format version.
 */
export function readCheckpoint(directory: string): CheckpointOffsets | undefined {
  let text: string
  try {
    text = readFileSync(join(directory, CHECKPOINT_FILE), 'utf8')
  } catch {
    return undefined
  }
  try {
    const parsed: unknown = JSON.parse(text)
    if (!isRecord(parsed) || parsed.version !== CHECKPOINT_VERSION) return undefined
    const offsets = parsed.offsets
    if (!isRecord(offsets)) return undefined
    const result = new Map<string, bigint>()
    for (const [name, offset] of Object.entries(offsets)) {
      if (typeof offset !== 'string' || !/^\d+$/.test(offset)) return undefined
      result.set(name, BigInt(offset))
    }
    return result
  } catch {
    return undefined
  }
}

/**
 * Writes the Checkpoint in `directory`, replacing any: a temporary file is written and synced,
 * then renamed over the Checkpoint, so a crash leaves either the old one or the new one.
 */
export function writeCheckpoint(directory: string, offsets: CheckpointOffsets): void {
  mkdirSync(directory, { recursive: true })
  const content = JSON.stringify({
    version: CHECKPOINT_VERSION,
    offsets: Object.fromEntries([...offsets].map(([name, offset]) => [name, String(offset)])),
  })
  const file = join(directory, CHECKPOINT_FILE)
  const temporary = `${file}.tmp`
  const fd = openSync(temporary, 'w')
  try {
    writeFileSync(fd, content)
    fsyncSync(fd)
  } finally {
    closeSync(fd)
  }
  renameSync(temporary, file)
  syncDirectory(directory)
}

/** Removes the Checkpoint in `directory`, if any, durably: the store is unclean from now on. */
export function removeCheckpoint(directory: string): void {
  rmSync(join(directory, CHECKPOINT_FILE), { force: true })
  syncDirectory(directory)
}

/** Makes a rename or removal in `directory` durable. Windows can't sync a directory. */
function syncDirectory(directory: string): void {
  if (process.platform === 'win32') return
  let fd: number
  try {
    fd = openSync(directory, 'r')
  } catch {
    return // the directory doesn't exist: nothing to sync
  }
  try {
    fsyncSync(fd)
  } finally {
    closeSync(fd)
  }
}

/**
 * The Checkpoint of one Task's persistent State Stores. It tracks the Changelog offset each store
 * has applied up to, and whether the Checkpoint file on disk still matches the stores (clean).
 */
export class TaskCheckpoint {
  readonly #directory: string
  /** The persistent stores, by name. */
  readonly #stores = new Map<string, StateStore>()
  readonly #offsets = new Map<string, bigint>()
  /** Whether the Checkpoint file exists and matches the stores. */
  #clean: boolean

  /** Reads the Checkpoint in `directory`, if there is one. */
  constructor(directory: string) {
    this.#directory = directory
    const found = readCheckpoint(directory)
    this.#clean = found !== undefined
    for (const [name, offset] of found ?? []) this.#offsets.set(name, offset)
  }

  /**
   * Adds the persistent store `name`, returning the store to use in its place: its first write
   * after a Checkpoint removes the Checkpoint first.
   */
  track(name: string, store: StateStore): StateStore {
    this.#stores.set(name, store)
    return guarded(store, () => this.#dirty())
  }

  /** The offset to restore store `name` from, or undefined to wipe and fully restore it. */
  offset(name: string): bigint | undefined {
    return this.#clean ? this.#offsets.get(name) : undefined
  }

  /**
   * Records that store `name` has applied its Changelog up to `offset` (the next offset), and
   * that every write to it so far is in its Changelog.
   */
  advance(name: string, offset: bigint): void {
    if (this.#stores.has(name)) this.#offsets.set(name, offset)
  }

  /**
   * Writes the Checkpoint if a store was written since the last one: flushes the stores, then
   * writes their offsets. Only call it once the Changelog records of every store write so far are
   * acknowledged, and with each store's offset advanced past them.
   */
  write(): void {
    if (this.#clean) return
    const offsets = new Map<string, bigint>()
    for (const [name, store] of this.#stores) {
      const offset = this.#offsets.get(name)
      // A store with no offset yet stays unclean; it is left out of the Checkpoint.
      if (offset !== undefined) offsets.set(name, offset)
      store.flush?.()
    }
    writeCheckpoint(this.#directory, offsets)
    this.#clean = true
  }

  #dirty(): void {
    if (!this.#clean) return
    removeCheckpoint(this.#directory)
    this.#clean = false
  }
}

/** `store`, calling `dirty` before each write that could make it differ from its Checkpoint. */
function guarded(store: StateStore, dirty: () => void): StateStore {
  let inTransaction = false
  return {
    get: (key) => store.get(key),
    put(key, value) {
      if (!inTransaction) dirty()
      store.put(key, value)
    },
    delete(key) {
      if (!inTransaction) dirty()
      store.delete(key)
    },
    all: () => store.all(),
    range: (from, to) => store.range(from, to),
    begin() {
      store.begin()
      inTransaction = true
    },
    commit() {
      dirty()
      store.commit()
      inTransaction = false
    },
    rollback() {
      store.rollback()
      inTransaction = false
    },
    flush: () => store.flush?.(),
    close: () => store.close(),
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}
