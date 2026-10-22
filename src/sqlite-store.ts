// Internal: the SQLite State Store (ADR-0005), the default durable store. One database file per
// State Store per Task, in the Task's state directory (ADR-0009).

import { mkdirSync } from 'node:fs'
import { join } from 'node:path'
import Database from 'better-sqlite3'
import type { StateStore } from './state-store.js'

type Row = { key: Buffer; value: Buffer }

/**
 * Opens the store `name` in `<directory>/<name>.sqlite`, creating the directory and database if
 * needed. Without a directory the database is kept in memory.
 */
export function openSqliteStore(directory: string | undefined, name: string): StateStore {
  let file = ':memory:'
  if (directory !== undefined) {
    mkdirSync(directory, { recursive: true })
    file = join(directory, `${name}.sqlite`)
  }
  return new SqliteStore(new Database(file), directory !== undefined)
}

class SqliteStore implements StateStore {
  readonly persistent: boolean
  readonly #db: Database.Database
  readonly #get: Database.Statement<[Buffer], Row>
  readonly #put: Database.Statement<[Buffer, Buffer]>
  readonly #delete: Database.Statement<[Buffer]>
  readonly #clear: Database.Statement<[]>
  readonly #all: Database.Statement<[], Row>
  readonly #from: Database.Statement<[Buffer], Row>
  readonly #to: Database.Statement<[Buffer], Row>
  readonly #between: Database.Statement<[Buffer, Buffer], Row>

  constructor(db: Database.Database, persistent: boolean) {
    this.persistent = persistent
    this.#db = db
    // WAL with synchronous=NORMAL survives a process crash; an OS crash may lose the last
    // commits, which Restore from the Changelog covers (ADR-0009).
    db.pragma('journal_mode = WAL')
    db.pragma('synchronous = NORMAL')
    // Keys are compared as BLOBs: memcmp, shorter prefix first, which is the key byte order.
    db.exec(
      'CREATE TABLE IF NOT EXISTS entries (key BLOB PRIMARY KEY, value BLOB NOT NULL) WITHOUT ROWID',
    )
    this.#get = db.prepare('SELECT key, value FROM entries WHERE key = ?')
    this.#put = db.prepare(
      'INSERT INTO entries (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value',
    )
    this.#delete = db.prepare('DELETE FROM entries WHERE key = ?')
    this.#clear = db.prepare('DELETE FROM entries')
    this.#all = db.prepare('SELECT key, value FROM entries ORDER BY key')
    this.#from = db.prepare('SELECT key, value FROM entries WHERE key >= ? ORDER BY key')
    this.#to = db.prepare('SELECT key, value FROM entries WHERE key < ? ORDER BY key')
    this.#between = db.prepare(
      'SELECT key, value FROM entries WHERE key >= ? AND key < ? ORDER BY key',
    )
  }

  get(key: Uint8Array): Uint8Array | undefined {
    const row = this.#get.get(buffer(key))
    return row === undefined ? undefined : new Uint8Array(row.value)
  }

  put(key: Uint8Array, value: Uint8Array): void {
    this.#put.run(buffer(key), buffer(value))
  }

  delete(key: Uint8Array): void {
    this.#delete.run(buffer(key))
  }

  // Without a WHERE clause SQLite truncates the table instead of deleting row by row.
  clear(): void {
    this.#clear.run()
  }

  all(): [Uint8Array, Uint8Array][] {
    return entries(this.#all.all())
  }

  // Rows are read eagerly: the connection can't write while a statement is being iterated.
  range(from: Uint8Array | undefined, to: Uint8Array | undefined): [Uint8Array, Uint8Array][] {
    if (from === undefined && to === undefined) return this.all()
    if (to === undefined) return entries(this.#from.all(buffer(from as Uint8Array)))
    if (from === undefined) return entries(this.#to.all(buffer(to)))
    return entries(this.#between.all(buffer(from), buffer(to)))
  }

  begin(): void {
    if (this.#db.inTransaction) throw new Error('A transaction is already open')
    this.#db.exec('BEGIN IMMEDIATE')
  }

  commit(): void {
    if (!this.#db.inTransaction) throw new Error('No transaction is open')
    this.#db.exec('COMMIT')
  }

  rollback(): void {
    if (!this.#db.inTransaction) throw new Error('No transaction is open')
    this.#db.exec('ROLLBACK')
  }

  // A FULL WAL checkpoint syncs the WAL and copies it into the database file, which is synced too:
  // synchronous=NORMAL alone doesn't sync on commit.
  flush(): void {
    if (this.persistent) this.#db.pragma('wal_checkpoint(FULL)')
  }

  close(): void {
    this.#db.close()
  }
}

/** A Buffer over the same memory, for binding as a BLOB (SQLite copies it). */
function buffer(bytes: Uint8Array): Buffer {
  return Buffer.from(bytes.buffer, bytes.byteOffset, bytes.byteLength)
}

/** Rows as plain Uint8Arrays the caller owns. */
function entries(rows: Row[]): [Uint8Array, Uint8Array][] {
  return rows.map(({ key, value }) => [new Uint8Array(key), new Uint8Array(value)])
}
