# 27: Full Restore from the Changelog

**What to build:** Before a Task processes records, its State Stores are fully restored from their Changelog, so `count()` results are correct after a SIGKILL and restart.

**Blocked by:** 13

**Status:** done

- [x] Full Restore from the Changelog before a Task processes
- [x] Restored state includes deletes (tombstones)
- [x] Integration test: `count()` survives SIGKILL under at-least-once (document the precise assertion: counts may only over-count by duplicates of uncommitted input)

Split from the original ticket 13.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 15

- **Client bug (@platformatic/kafka 2.14.0):** a read_committed `consume()` delivers aborted records when the abort marker is not in the same fetch response (reproduced with old timestamps forcing a segment roll; Java consumer read 0). Check whether the low-level `fetch()` path used for Restore is affected. If it is, filter using the fetch response aborted-transaction lists yourself, tracked across responses, and test it with an aborted Changelog transaction spanning fetches.

## Comments

- **Module:** `src/restore.ts`. `restoreStores(stores, partition, reader)` wipes each store, then replays the Task's Changelog partition into it, one store transaction per fetched chunk. Keys are deserialized with the store's key Serde and re-encoded with `StoreNode.keyEncoding` (default `singleKeyEncoding`); Tombstones delete. `ChangelogReader` is the seam (unit tests use a fake one). `createChangelogReader` is the Kafka restore consumer: a `Consumer` used only through the low-level `fetch()` (no group join), `read_committed`, from the log start offset to the read_committed end offset (LSO) taken when the read starts. Records past that end in the last batch are applied too (they're committed). Errors: `OFFSET_OUT_OF_RANGE` restarts from the log start; leader errors refresh metadata and retry.
- **Client bug check (fetch path): affected.** `Consumer.fetch()` runs the same `#filterUncommittedMessages` as `consume()`, and that filter ignores an aborted transaction whose abort marker isn't in the same response. Verified: with a 1 KB `partitionMaxBytes`, an aborted 3-record transaction came back in full when our own filter was turned off. `AbortedTransactions` in `restore.ts` does what the Java consumer does: it collects the response's aborted-transaction list (producer ID + first offset), treats a producer's transactional batches as aborted from that first offset until its abort marker, and keeps that state across responses. The integration test `The restore consumer › skips an aborted transaction whose abort marker comes in a later fetch` covers it.
- **Wiring:** the Worker creates a restore consumer (`<clientId>-restore`) when the app has State Stores, and closes it with its other clients. `TaskOptions.restore` is the Task's first queued job, so records (and timers) given to a new Task wait inside the Task until Restore finishes. Other Tasks in the batch keep processing; the batch commit waits for all of them. This is ADR-0009's "hold the Task's records in memory". Ticket 19 can reuse `restoreStores` and the reader, and only needs to change *when* the batch waits.
- **Full Restore wipes local state first**, including persisted SQLite files: with no Checkpoint (ticket 14), a local store can't be trusted. Under at-least-once it may contain writes whose Changelog send never happened before a crash. Ticket 14 should keep the wipe only for a missing or unclean Checkpoint.
- **SIGKILL assertion** (`at-least-once-crash.test.ts`, in-memory store so only Restore can bring counts back): every key's final count is ≥ its number of input records (nothing lost), and the summed over-count is ≤ `kills × commitMaxRecords`. Only the one batch per crash whose Changelog send landed but whose offsets weren't committed can be counted twice. The test fails with Restore disabled.
- Concerns for later: (1) Restore runs inside a batch, so a long Restore holds the batch open past `commitIntervalMs`. That's fine for at-least-once, but exactly-once with stores (ticket 16+) would hold a transaction open longer than its timeout. It needs ticket 19's non-blocking Restore, or a Restore before a transaction begins. (2) The Application State doesn't show `restoring` yet. (3) Restore re-encodes keys through the key Serde, so a Serde that doesn't round-trip exactly corrupts the store key (ADR-0015 already notes this). (4) Revoked or reassigned Tasks aren't closed or re-restored (rebalances are tickets 19/28).

