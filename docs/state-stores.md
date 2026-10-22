# State Stores

A **State Store** is durable keyed state owned by exactly one Task at a time. Each Task has its own copy of every store. DSL aggregations create stores for you (name them with `{ store: 'name' }`); Processors use stores registered with `app.store(name, serdes)` through `ctx.store(...)`.

## Changelog, Restore and Checkpoints

Every write to a State Store is also written to its **Changelog**, a compacted Kafka topic (`<applicationId>-<store>-changelog`), in the same Kafka transaction as the batch's outputs under exactly-once. The Changelog is the source of truth; the local store is a derived copy. Deleting a key writes a **Tombstone** so deletes survive.

When a Task starts on a Worker (after a restart, a rebalance, or a disk loss) its stores are rebuilt by **Restore**:

- A persistent store (SQLite in a `stateDir`) keeps a **Checkpoint**, the Changelog offset it has applied up to. Restore applies only what comes after it ([ADR-0009](./adr/0009-local-state-checkpoints-and-restore.md)).
- If the Checkpoint is missing or unclean (a crash mid-batch, a copied directory), the store is wiped and fully restored, so corrupt local state never leaks into results.
- A store without a persistent directory (no `stateDir`, or `stores.inMemory()`) is fully restored on every start.

A Task restores when its first record arrives, not at startup. While a Task restores, the Worker's other Tasks keep processing, and there is no limit on how many Tasks restore concurrently at startup. A starting Task holds at most `commitMaxRecords` records; past that it reads them again from the committed offsets after the Restore. See [observability](./observability.md) for the `restoring` state, the Restore events and metrics.

## Choosing a store

The default is SQLite (`better-sqlite3`) in the file `<store>.sqlite` of the Task's directory, `<stateDir>/<applicationId>/<task>/`; without a `stateDir`, an in-memory store. Pass a `storeSupplier` (DSL) or `supplier` (`app.store`) for something else:

```ts
import { createStreamsApp, serdes, stores } from 'kstreams-ts'

const app = createStreamsApp({ applicationId: 'cache', stateDir: './state' })

app
  .stream('in', { key: serdes.string, value: serdes.string })
  .groupByKey()
  // Keep this aggregation in memory instead of SQLite; every start is then a full Restore.
  .count({ store: 'counts', storeSupplier: stores.inMemory() })
  .toStream()
  .to('out', { key: serdes.string, value: serdes.number })
```

State Stores are a pluggable, byte-level interface (`StateStore`, [ADR-0005](./adr/0005-pluggable-stores-sqlite-default.md)): `get`, `put`, `delete`, `clear`, `all`, `range`, `begin`/`commit`/`rollback`, `close`, and optional `persistent` and `flush`. The engine lines the store's transactions up with Kafka transactions, so a store must make writes durable only on `commit` and undo them on `rollback`. `clear` removes every entry; the engine uses it to wipe a store before a full Restore, so it shouldn't read the entries (SQLite runs `DELETE FROM`). A supplier receives the store's name, the Application ID, the Task number and the Task directory.

## The typed view

`KeyValueStore<K, V>` gives `get`, `put` (a null value deletes), `delete`, `all()` and `range(from, to)`. Both ends of `range` are included and results are in ascending key order. Keys are ordered by value, not by their serialized bytes: `null`, then booleans, numbers numerically, strings by code point, byte arrays, arrays element by element; other types (objects) last, by their Serde bytes ([ADR-0015](./adr/0015-order-preserving-store-key-encoding.md)). `put` and `delete` throw on a `null` or `undefined` key, since the compacted Changelog needs a key; the Test Driver does the same. Don't write to a store while iterating it.

A store uses each key as its key Serde gives it back: the key is serialized and deserialized first, exactly as it would be when the store is restored from its Changelog. For most keys this changes nothing. When the Serde doesn't round-trip a key, the store holds the round-tripped key: a `Date` key with `serdes.json()` is stored, and listed by `all()` and `range()`, as its ISO string; `NaN` with `serdes.json()` as `null`; a string with a lone surrogate with `serdes.string` with U+FFFD in its place. `get`, `put` and `delete` with the original key still reach that entry, so results are the same before and after a Restore. Each store access serializes and deserializes its key once.

Stores are not readable from outside the app in 0.1. In tests, read them through the [Test Driver](./test-driver.md).
