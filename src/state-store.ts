// State Stores: the pluggable byte-level interface (ADR-0005), the built-in in-memory store, and
// the typed key-value view Processors and the Test Driver use on top of it.

import { join } from 'node:path'
import type { Serde, SerdeContext, Serdes } from './serde.js'
import { openSqliteStore } from './sqlite-store.js'
import { keyOf, storeKey } from './store-key.js'
import { UnloggedWriteError } from './sync-dsl.js'

/**
 * A key-value State Store as implementations provide it: keys and values are bytes, already
 * serialized. Implement it to plug in your own storage; every implementation must pass the State
 * Store contract suite.
 *
 * Writes outside a transaction apply immediately. Between `begin` and `commit` they are only made
 * durable on `commit`, and `rollback` undoes every write since `begin`. The engine lines these
 * transactions up with Kafka transactions (ADR-0002). Transactions don't nest.
 *
 * A store copies the bytes it is given and returns bytes the caller may keep and change.
 */
export interface StateStore {
  /** The value of `key`, or undefined if there is none. */
  get(key: Uint8Array): Uint8Array | undefined
  /** Sets the value of `key`, replacing any value it had. */
  put(key: Uint8Array, value: Uint8Array): void
  /** Removes `key`. Does nothing if it has no value. */
  delete(key: Uint8Array): void
  /**
   * Every entry, in ascending key byte order (unsigned, shorter prefix first). Don't write to the
   * store while iterating.
   */
  all(): Iterable<[key: Uint8Array, value: Uint8Array]>
  /**
   * The entries whose keys are at least `from` and less than `to`, in ascending key byte order.
   * An undefined bound leaves that end open. Don't write to the store while iterating.
   */
  range(
    from: Uint8Array | undefined,
    to: Uint8Array | undefined,
  ): Iterable<[key: Uint8Array, value: Uint8Array]>
  /** Starts a transaction. Throws if one is already open. */
  begin(): void
  /** Makes the open transaction's writes durable. Throws if no transaction is open. */
  commit(): void
  /** Undoes every write of the open transaction. Throws if no transaction is open. */
  rollback(): void
  /** Releases the store's resources. The store is not used after this. */
  close(): void
  /**
   * Whether the store keeps its contents across restarts, in the Task's state directory. The
   * engine keeps a Checkpoint for a persistent store and, on a restart, restores only the part of
   * its Changelog after the Checkpoint; a store whose Checkpoint is missing or unclean is wiped and
   * fully restored (ADR-0009). Stores that aren't persistent are fully restored on every start.
   * Defaults to false.
   */
  readonly persistent?: boolean
  /**
   * Makes every committed write durable, surviving an OS crash. The engine calls it before it
   * writes a Checkpoint of a persistent store. Optional: without it, writes are taken as durable
   * once made.
   */
  flush?(): void
}

/** What a State Store supplier is told about the store it opens. */
export interface StateStoreContext {
  /** The name the State Store was given in the Streams App. */
  readonly name: string
  /** The Application ID of the Streams App. */
  readonly applicationId: string
  /** The Task the store belongs to: its partition number. */
  readonly task: number
  /**
   * The Task's state directory, `<stateDir>/<applicationId>/<task>`, for the store's files. It
   * may not exist yet. Undefined when no state directory is configured (e.g. in the Test Driver),
   * meaning state is kept in memory.
   */
  readonly directory: string | undefined
}

/** Opens a State Store. Called once per State Store per Task. */
export type StateStoreSupplier = (context: StateStoreContext) => StateStore

/** Built-in State Store suppliers. */
export const stores = {
  /** Keeps the State Store in memory. Its contents are lost when the process ends. */
  inMemory(): StateStoreSupplier {
    return () => new InMemoryStore()
  },
  /**
   * The default durable State Store: a SQLite database (`better-sqlite3`) in the file
   * `<store>.sqlite` of the Task's state directory, which is created if needed. It is persistent:
   * a restart restores only the part of its Changelog after its Checkpoint. Without a state
   * directory the database is kept in memory, and is fully restored on every start.
   */
  sqlite(): StateStoreSupplier {
    return ({ name, directory }) => openSqliteStore(directory, name)
  },
}

/**
 * Internal: the supplier of State Stores that aren't given one. SQLite is the default durable
 * store, used when the Task has a state directory; otherwise the store is kept in memory.
 */
export const defaultStoreSupplier: StateStoreSupplier = (context) =>
  context.directory === undefined ? new InMemoryStore() : stores.sqlite()(context)

/** Internal: the state directory of a Task, under the configured state directory. */
export function taskDirectory(stateDir: string, applicationId: string, task: number): string {
  return join(stateDir, applicationId, String(task))
}

/**
 * Typed access to a key-value State Store, as Processors get it from `ctx.store`. Keys and values
 * are serialized with the store's Serdes.
 */
export interface KeyValueStore<K, V> {
  /** The value of `key`, or undefined if there is none. */
  get(key: K): V | undefined
  /** Sets the value of `key`. A null value deletes the key, like a tombstone. */
  put(key: K, value: V): void
  /** Removes `key`. Does nothing if it has no value. */
  delete(key: K): void
  /** Every entry, in ascending key order (see `range`). */
  all(): [key: K, value: V][]
  /**
   * The entries with keys from `from` to `to`, both included, in ascending key order. Empty when
   * `from` comes after `to`.
   *
   * Keys are ordered by value, not by their Serde bytes: null, then false and true, numbers
   * numerically, strings by Unicode code point, byte arrays by unsigned bytes, then arrays of
   * these element by element. Keys of other types (e.g. objects) come last, ordered by their
   * Serde bytes.
   */
  range(from: K, to: K): [key: K, value: V][]
}

declare const storeTypes: unique symbol

/** A State Store registered with `app.store`. Pass it to `ctx.store` for typed access. */
export interface NamedStore<K, V> {
  readonly name: string
  /** Type-only: the key and value types of the store. */
  readonly [storeTypes]?: { key: K; value: V }
}

/** Internal: how the typed view of a State Store turns its keys into store keys and back. */
export interface KeyEncoding<K> {
  encode(key: K, context: SerdeContext): Uint8Array
  decode(bytes: Uint8Array, context: SerdeContext): K
}

/** Internal: the store key encoding of a key-value store, a single part (ADR-0015). */
export function singleKeyEncoding<K>(serde: Serde<K>): KeyEncoding<K> {
  return {
    encode: (key, context) => storeKey(key, serde, context),
    decode: (bytes, context) => keyOf(bytes, serde, context),
  }
}

/**
 * Internal: receives every write to a State Store as its Changelog record: the key as the key
 * Serde's bytes (not the store key, ADR-0015) and the value as the value Serde's bytes, or null
 * for a delete (a Tombstone).
 */
export type ChangelogWriter = (key: Uint8Array, value: Uint8Array | null) => void

/** Internal: the entries of a typed store view whose store keys are in `[from, to)`, by view. */
const storeKeyRanges = new WeakMap<
  KeyValueStore<unknown, unknown>,
  (from: Uint8Array | undefined, to: Uint8Array | undefined) => [unknown, unknown][]
>()

/**
 * Internal: the entries of a typed store view whose store keys (ADR-0015) are at least `from` and
 * less than `to`, decoded. An undefined bound leaves that end open. For the DSL's own Processors,
 * e.g. to find expired windows with one range scan.
 */
export function rangeByStoreKey<K, V>(
  view: KeyValueStore<K, V>,
  from: Uint8Array | undefined,
  to: Uint8Array | undefined,
): [K, V][] {
  const range = storeKeyRanges.get(view as KeyValueStore<unknown, unknown>)
  if (range === undefined) throw new TypeError('Not a State Store view created by keyValueStore')
  return range(from, to) as [K, V][]
}

/**
 * Internal: wraps a byte-level store with the store's Serdes. Keys are encoded with `encoding`,
 * by default as a single part encoded with the key Serde as fallback. With a `changelog`, every
 * put and delete is also written to it.
 */
export function keyValueStore<K, V>(
  store: StateStore,
  serdes: Serdes<K, V>,
  context: SerdeContext,
  encoding: KeyEncoding<K> = singleKeyEncoding(serdes.key),
  changelog?: ChangelogWriter,
): KeyValueStore<K, V> {
  const key = (k: K) => encoding.encode(k, context)
  const value = (bytes: Uint8Array) => serdes.value.deserialize(bytes, context)
  /** Writes `bytes` (null deletes) under `k`, and to the Changelog if there is one. */
  const write = (k: K, bytes: Uint8Array | null) => {
    if (changelog !== undefined && (k === null || k === undefined)) {
      throw new Error(
        `Cannot write a ${k} key to a State Store: its Changelog "${context.topic}" is compacted, which needs a key`,
      )
    }
    // Everything that can fail on a key happens before the store changes.
    const local = key(k)
    const logged = changelog === undefined ? undefined : serdes.key.serialize(k, context)
    if (bytes === null) store.delete(local)
    else store.put(local, bytes)
    if (changelog === undefined || logged === undefined) return
    try {
      changelog(logged, bytes)
    } catch (cause) {
      throw new UnloggedWriteError('A State Store write could not be logged to its Changelog', {
        cause,
      })
    }
  }
  const entries = (found: Iterable<[Uint8Array, Uint8Array]>): [K, V][] =>
    Array.from(found, ([k, v]) => [encoding.decode(k, context), value(v)])
  const view: KeyValueStore<K, V> = {
    get(k) {
      const bytes = store.get(key(k))
      return bytes === undefined ? undefined : value(bytes)
    },
    put(k, v) {
      write(k, v === null ? null : serdes.value.serialize(v, context))
    },
    delete(k) {
      write(k, null)
    },
    all() {
      return entries(store.all())
    },
    range(from, to) {
      // Store keys are made of self-delimiting parts whose tags are never 0, so `to` followed by
      // a 0 byte is the first byte string after it: the exclusive end that includes `to`.
      const end = key(to)
      const after = new Uint8Array(end.length + 1)
      after.set(end)
      return entries(store.range(key(from), after))
    },
  }
  storeKeyRanges.set(view as KeyValueStore<unknown, unknown>, (from, to) =>
    entries(store.range(from, to)),
  )
  return view
}

interface Entry {
  key: Uint8Array
  value: Uint8Array
}

/** A copy sharing no memory with `bytes`; `.slice()` on a Node Buffer would share it. */
const copy = (bytes: Uint8Array): Uint8Array => Uint8Array.from(bytes)

class InMemoryStore implements StateStore {
  /** Entries by key as hex, whose string order is the key byte order. */
  readonly #entries = new Map<string, Entry>()
  /** While a transaction is open: the entry each written key had before it, if any. */
  #undo: Map<string, Entry | undefined> | undefined

  get(key: Uint8Array): Uint8Array | undefined {
    const found = this.#entries.get(hex(key))
    return found === undefined ? undefined : copy(found.value)
  }

  put(key: Uint8Array, value: Uint8Array): void {
    const id = hex(key)
    this.#remember(id)
    this.#entries.set(id, { key: copy(key), value: copy(value) })
  }

  delete(key: Uint8Array): void {
    const id = hex(key)
    this.#remember(id)
    this.#entries.delete(id)
  }

  all(): [Uint8Array, Uint8Array][] {
    return this.range(undefined, undefined)
  }

  range(from: Uint8Array | undefined, to: Uint8Array | undefined): [Uint8Array, Uint8Array][] {
    const low = from === undefined ? undefined : hex(from)
    const high = to === undefined ? undefined : hex(to)
    return [...this.#entries]
      .filter(([id]) => (low === undefined || id >= low) && (high === undefined || id < high))
      .sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
      .map(([, { key, value }]) => [copy(key), copy(value)])
  }

  begin(): void {
    if (this.#undo !== undefined) throw new Error('A transaction is already open')
    this.#undo = new Map()
  }

  commit(): void {
    this.#transaction()
    this.#undo = undefined
  }

  rollback(): void {
    for (const [id, entry] of this.#transaction()) {
      if (entry === undefined) this.#entries.delete(id)
      else this.#entries.set(id, entry)
    }
    this.#undo = undefined
  }

  close(): void {
    this.#entries.clear()
    this.#undo = undefined
  }

  #remember(id: string): void {
    if (this.#undo !== undefined && !this.#undo.has(id)) this.#undo.set(id, this.#entries.get(id))
  }

  #transaction(): Map<string, Entry | undefined> {
    if (this.#undo === undefined) throw new Error('No transaction is open')
    return this.#undo
  }
}

function hex(bytes: Uint8Array): string {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength).toString('hex')
}
