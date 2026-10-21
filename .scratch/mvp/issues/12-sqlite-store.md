# 12: SQLite State Store

**What to build:** The SQLite State Store (`better-sqlite3`) passes the same contract suite as the in-memory store, plus order-preserving key encoding and range scans that windows will rely on.

**Blocked by:** 10

**Status:** done

- [x] `better-sqlite3` store passes the contract suite
- [x] Deterministic, order-preserving key byte encoding independent of the key Serde
- [x] Range scans in key order (contract suite extended, in-memory store too)
- [x] SQLite is the default durable store

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/12`. Public API:
  - **`StateStore.range(from, to)`** (byte level): entries with `from <= key < to` in ascending key byte order; an undefined bound is open. In-memory and SQLite both implement it; the contract suite (`test/state-store-contract.ts`) covers it, including bounds that aren't keys, open bounds, empty ranges, uncommitted writes and rollback. The contract runs against in-memory, SQLite on a file and SQLite with no state directory.
  - **`KeyValueStore.range(from, to)`** (typed, `ctx.store`): both ends included, like Kafka Streams; `TestStore` now has `range` too. `all()`/`range()` return keys in key order (see below), no longer in Serde-byte order.
  - **`stores.sqlite()`**: `better-sqlite3` (13.x, prebuilt Node-API binaries, no install script; `@types/better-sqlite3` for types). One database per State Store per Task at `<directory>/<store>.sqlite`, with a `WITHOUT ROWID` table `entries (key BLOB PRIMARY KEY, value BLOB NOT NULL)`. WAL + `synchronous=NORMAL` (a process crash loses nothing that was committed; an OS crash may lose the last commits, which Checkpoints/Restore cover). `begin` is `BEGIN IMMEDIATE`; manual `BEGIN`/`COMMIT` stays open across `await`s, which the ADR-0002 commit cycle needs. Without a directory it uses `:memory:`. Range reads are eager (`.all()`), because the connection can't write while a statement is iterating.
  - **`StateStoreContext`** is now `{ name, applicationId, task, directory }`. `task` is the partition number. `directory` is `<stateDir>/<applicationId>/<task>` (it may not exist yet; the SQLite store creates it), or `undefined` when no state directory is configured.
  - **SQLite is the default durable store:** a store without a supplier is SQLite when its Task has a state directory, and in-memory otherwise (`defaultStoreSupplier`, internal). `StreamsAppConfig.stateDir?` was added (exposed as `app.stateDir`); `createTask(..., { task, stateDir })` takes it. Nothing reads `app.stateDir` yet: the Kafka runtime should pass it to `createTask`.
  - **Test Driver decision:** it keeps stores in memory and ignores `app.stateDir`, so every Test Driver starts empty and leaves no files behind. `createTestDriver(app, { stateDir })` (new `TestDriverOptions`) opts in to the durable path as Task 0, e.g. with a temp dir.
- **Store key encoding** (`src/store-key.ts`, internal; the format is documented in the file header). A store key is a sequence of self-delimiting parts, each a tag byte and its payload:
  - `0x01` null; `0x02`/`0x03` false/true
  - `0x10` + 8 bytes: a number as IEEE 754 big-endian, with the sign bit flipped for positives and every bit flipped for negatives. `-0` is encoded as `0`, and every NaN as the canonical NaN, which sorts after Infinity.
  - `0x20` string as UTF-8 (code point order, which differs from JS UTF-16 `<` above U+FFFF); `0x30` bytes. Both are escaped (`0x00` becomes `0x00 0xFF`) and terminated by `0x00`.
  - `0x40` array: its elements' parts, then a `0x00` terminator, so a shorter array sorts before the arrays it is a prefix of.
  - `0x50` fallback: the key **Serde bytes**, escaped and terminated, for keys with no encoding of their own (objects, bigints, ill-formed strings, arrays holding any of these). These sort after every native value, by bytes.
  - Values of different types sort by tag. A key-value store key is one part: `storeKey(key, serde, ctx)`, decoded with `keyOf`. A native key (e.g. a string) never goes through the key Serde, so a key Serde is no longer called for those keys.
- **For ticket 22 (windows):** encode the window store key as `encodeKey([part, windowStart])`, where `part` is the key itself if `isOrderedValue(key)`, else `new SerializedKey(serde bytes)`. To scan one key's windows from t1 to t2, use `range(encodeKey([part, t1]), encodeKey([part, t2]) + 0x00)`. To scan all of a key's windows, use `range(encodeKey([part]), prefixEnd(encodeKey([part])))`. Don't use a naive byte-prefix match: `'a\0'` starts with the bytes of `'a'`, and `prefixEnd` excludes it. A retention sweep by time across all keys would need a time-first layout or a second index; that's your call.
- **For ticket 13 (Changelog):** the Changelog key should stay the key's **Serde bytes** (Kafka partitioning and compaction work on them). Restore maps a Changelog key to a store key with `storeKey(serde.deserialize(bytes), serde, ctx)`. This relies on the key Serde round-tripping, which the built-ins do.
- **For ticket 14 (Checkpoints):** the SQLite store has no Checkpoint/metadata table yet. Add one in the same database, written in the same transaction as the data. There's no wipe-on-unclean logic either. The Task still never calls `close()`/`begin()`/`commit()` (unchanged from ticket 10).
- The on-disk format (table layout plus key encoding) is now a compatibility surface. Changing it needs a wipe and Restore.
