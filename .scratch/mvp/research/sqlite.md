# SQLite for State Stores — better-sqlite3 13.x (and node:sqlite)

Machine: Node **v26.8.2**, macOS arm64. Experiment:
`/Users/igorshalygin/.claude/jobs/e36bc646/tmp/explore/sqlite/scripts/exp.mjs` (all items below **[V]**
verified there unless marked [U]).

## Install — clean, no compiler [V]
- `better-sqlite3@13.0.3` (latest, 2026-08-05). v13 is built on **node-addon-api (Node-API)** and ships
  **prebuilt binaries inside the npm tarball** (`prebuilds/{darwin,linux,linuxmusl,win32}-{x64,arm64}.node`).
  No install script, no node-gyp, no download at install time (`"gypfile": false`). `npm i` took 3 s.
  Node-API → ABI-stable across Node majors, so Node 26 works (`engines: node >=22`).
- Deps: only `node-addon-api`. Bundled SQLite **3.53.4**.
- CommonJS package; `import Database from 'better-sqlite3'` works from ESM.
- **Types are not bundled**: use `@types/better-sqlite3` (latest 9.6.0, updated 2026-10-04) [U: whether
  it covers every v13 change; the core API below is unchanged from older majors].

## Core API
```ts
import Database from 'better-sqlite3'
const db = new Database(file /* ':memory:' ok */, { /* readonly, fileMustExist, timeout: 5000 */ })
db.pragma('journal_mode = WAL', { simple: true })   // → 'wal'
db.pragma('synchronous = NORMAL')                   // WAL + NORMAL: durable on app crash, may lose last txns on OS crash
db.exec(`CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID;
         CREATE TABLE IF NOT EXISTS meta (name TEXT PRIMARY KEY, value)`)

const put = db.prepare('INSERT INTO kv (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v')
const get = db.prepare('SELECT v FROM kv WHERE k = ?')
put.run(keyBuf, valBuf)            // → { changes: 1, lastInsertRowid: 0 }  (0 for WITHOUT ROWID)
get.get(keyBuf)?.v                 // → Buffer (Buffer.isBuffer true) | undefined when no row
db.prepare('SELECT k FROM kv ORDER BY k').pluck().all()   // pluck(): first column only
for (const row of db.prepare('SELECT k, v FROM kv WHERE k >= ? AND k < ? ORDER BY k').iterate(lo, hi)) {}
db.close()
```
- Statements are synchronous; prepare once, reuse. `.get/.all/.iterate/.run`, `.pluck()`, `.raw()`
  (arrays), `.expand()`, `.safeIntegers()`.
- While an `.iterate()` is open, **writes** on the same connection throw "This database connection is
  busy executing a query" (reads via `.get()` are fine) [V] — for range-scan-then-write, `.all()` first.
- Integers: default read returns JS `number` → **loses precision** above 2^53 (wrote
  9007199254740993n, read 9007199254740992). Use `.safeIntegers()` on the statement (or
  `db.defaultSafeIntegers(true)`) for Kafka offsets → `bigint`. Binding a `bigint` parameter works.

## Transactions
```ts
const putMany = db.transaction((entries: [Buffer, Buffer][]) => { for (const [k, v] of entries) put.run(k, v); return entries.length })
putMany(entries)         // BEGIN … COMMIT; any throw → ROLLBACK and rethrow. Returns fn's value.
putMany.immediate(entries) // also .deferred / .exclusive
```
- The function **must be synchronous**: returning a promise throws
  `TypeError: Transaction function cannot return a promise`. (Fits ADR-0012.)
- Nested `db.transaction()` calls (or one called inside a manual `BEGIN`) become SAVEPOINTs: inner throw
  rolls back only the inner part; outer still commits.
- **Manual control spanning `await`s** (what the EOS batch loop needs — the SQLite txn must stay open
  while the Kafka txn is committed asynchronously):
```ts
db.exec('BEGIN IMMEDIATE')   // IMMEDIATE takes the write lock now
try {
  // … many synchronous put.run(...) across awaits …
  await kafkaTx.commit()
  db.exec('COMMIT')
} catch (e) { if (db.inTransaction) db.exec('ROLLBACK'); throw e }
```
  Verified: BEGIN, write, `await` a timer, `db.inTransaction === true`, ROLLBACK discards the write.
  Caveat: there's an unavoidable window where Kafka committed but SQLite COMMIT hasn't run (crash
  there) — that's what Checkpoints + "unclean store ⇒ wipe & restore" (ADR-0009) cover.

## BLOB keys & ordering [V]
`ORDER BY` on BLOB uses `memcmp` then length (shorter prefix first) — identical to
`Buffer.compare`. Observed order: `'' < 00 < 00ff < 01 < 7f < 7f00 < 80 < ff`. Range scans with
`k >= ? AND k < ?` on a BLOB PRIMARY KEY use the index. Notes:
- Keep the column type BLOB and always bind `Buffer`/`Uint8Array` — mixing TEXT/INTEGER values in the
  same column sorts by storage class (INTEGER < TEXT < BLOB), not bytes.
- Empty `Buffer` binds as a zero-length BLOB (sorted first), distinct from `NULL`.
- `WITHOUT ROWID` tables are a good fit for kv stores (clustered on the key).
- For serialized keys to sort meaningfully, the key serde must be order-preserving (e.g. big-endian ints).

## worker_threads [V]
Two workers each opened their own `new Database(file)` on the same WAL file concurrently with the main
thread's connection and inserted rows; all succeeded. Rules:
- A `Database` object can't be shared/transferred between threads — open one per thread (each Worker
  owns its Tasks' stores anyway, ADR-0007).
- Native addon loads fine in workers (Node-API, context-aware).
- Cross-connection writes to one file serialize on SQLite's lock; set `timeout` (busy timeout, default
  5000 ms) if two threads could ever write the same file. With one store file per Task this doesn't arise.

## node:sqlite on Node 26 [V]
- Available **unflagged** (no warning printed) on v26.8.2: exports `DatabaseSync`, `StatementSync`,
  `Session`, `constants`, `backup`. Bundled SQLite 3.53.4.
- Stability: **1.2 – Release candidate** (docs `nodejs.org/docs/latest-v26.x/api/sqlite.json`, added
  v22.5.0). The current v26 docs show the class as `Database`; v26.8.2 only exports `DatabaseSync` → API
  still moving.
- Differences that matter: BLOBs come back as `Uint8Array` (not Buffer); **no `db.transaction()` helper**
  (use manual BEGIN/COMMIT); no `.pluck()`. Zero install cost, but RC status → stick with better-sqlite3
  for v1 (ADR-0005 stores are pluggable, so a node:sqlite store can be added later).
