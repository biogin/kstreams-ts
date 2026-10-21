# 10: `count()` with an in-memory State Store

**What to build:** A user writes `groupByKey().count({ store })` or uses `ctx.store(name)` in a Processor, and inspects the store's contents in the Test Driver. The State Store interface and its contract suite (seam 3) exist, with the in-memory store passing it.

**Blocked by:** 03, 06

**Status:** done

- [x] Pluggable key-value State Store interface (ADR-0005)
- [x] In-memory store passes the contract suite (get/put/delete, transactional rollback)
- [x] `groupByKey`, `count`. `ctx.store(name)` in Processors
- [x] Test Driver: read store contents

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/10`. Public API:
  - **Pluggable store (ADR-0005):** `StateStore` is byte-level (`get`/`put`/`delete`/`all()` in ascending unsigned key-byte order, `begin`/`commit`/`rollback`, `close`), synchronous (fits `better-sqlite3`). Writes outside a transaction apply immediately (autocommit); transactions don't nest; `begin` inside one or `commit`/`rollback` outside one throws. Stores copy bytes in and out. `StateStoreSupplier = (context: StateStoreContext) => StateStore`, called once per store per Task; `StateStoreContext` is `{ name }` for now — ticket 12 should add what SQLite needs (state directory, Task id) there. Built-in: `stores.inMemory()`.
  - **Contract suite (seam 3):** `test/state-store-contract.ts` exports `describeStateStoreContract(implementation, open)`; `test/in-memory-store.test.ts` runs it. Ticket 12 runs it for SQLite and extends it with range scans (in-memory `all()` already sorts by key bytes, so a `range(from, to)` is a small addition).
  - **Typed layer:** `KeyValueStore<K, V>` (`get`/`put`/`delete`/`all(): [K, V][]`) wraps a `StateStore` with the store's Serdes; `put(key, null)` deletes (tombstone). Serdes are told `topic: <applicationId>-<store>-changelog` (ticket 13 should use the same name, now `StoreNode.changelogTopic`).
  - **Processor API:** `app.store(name, serdes, { supplier? })` returns a `NamedStore<K, V>`; `ctx.store(namedStore)` is typed, `ctx.store<K, V>(name)` by name. Any Processor of the app can use any store (no per-Processor connection); unknown names throw. Store names follow the Application ID charset and are unique across `app.store` and DSL stores.
  - **DSL:** `stream.groupByKey(serdes?)` → `GroupedStream<K, V>` (throws without a key Serde, e.g. after `map`); `.count({ store?, storeSupplier? })` → `Table<K, number>` whose `store` is the State Store name (generated as `count-store-NNNN` when not given). Counts use `serdes.number`. The count Processor already forwards `{ key, count }` downstream, so ticket 11's `toStream()` only needs to expose that node; `GroupedStream` keeps only the key Serde today — `aggregate`/`reduce` will need the value Serde from `groupByKey(serdes)` too.
  - **Test Driver:** `driver.store(namedStore | name)` → `TestStore<K, V>` (`get`, `all`), decoded with the store's Serdes; throws for unknown stores.
- **Null values (open question):** like Kafka Streams, `count` skips records with a null key or a null value (tombstone): they aren't counted and nothing is forwarded. `groupByKey` itself doesn't filter. Operators are still typed as receiving `V` even though a tombstone reaches them at runtime as `null`; full tombstone typing is ticket 11.
- **Transactions are not driven by the engine yet:** the Task opens stores in autocommit mode and never calls `begin`/`commit`/`rollback`, so a record that fails mid-way leaves its earlier writes in the store (visible via `driver.store`). The commit cycle (tickets 08/15/16) should own `begin`/`commit`/`rollback` per batch, ADR-0002. Stores are also never `close()`d yet (no Test Driver/Task shutdown exists).
- Store keys are the key Serde's bytes for now; the order-preserving key encoding independent of the Serde (ADR-0005) is ticket 12. Default supplier is in-memory everywhere; ticket 12 makes SQLite the durable default.
- `ProcessorContext.store` changed from the `never` placeholder to overloads; `ctx.schedule` (ticket 21) untouched, and the "not implemented" test now covers only `schedule`.
