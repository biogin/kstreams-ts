# 13: Changelog writes and auto-created topics

**What to build:** Every State Store write (puts and deletes as tombstones) is recorded to an auto-created, compacted Changelog topic named `<applicationId>-<store>-changelog`, produced in the same commit cycle as the outputs.

**Blocked by:** 08, 12

**Status:** done

- [x] Changelog topics auto-created: compacted, source partition count, replication factor from config (default 3, tests use 1)
- [x] Every store put/delete is written to the Changelog partition matching the Task
- [x] Integration test: after running `count()`, the Changelog contents match the final store contents

Split from the original ticket 13; Restore is ticket 27.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 22

- Window stores: Changelog keys must use the windowed key Serde, and Restore (ticket 27) must re-encode with `StoreNode.keyEncoding`, not `storeKey`.

## Comments

- **Changelog record format** (Restore, ticket 27, rebuilds from this): key = the store's key Serde bytes (for window stores the windowed key Serde: key bytes + 8-byte big-endian window start), value = the value Serde bytes, Tombstone (null value) for `delete`/`put(k, null)`. Partition = the Task number (explicit, not the partitioner). Timestamp = wall-clock time of the write. No headers. Restore must re-encode keys with `StoreNode.keyEncoding` (default `singleKeyEncoding(serdes.key)`); the store key encoding (ADR-0015) never reaches Kafka.
- Logging happens in `keyValueStore` (`src/state-store.ts`, `ChangelogWriter`), so every store type is covered; the Task passes `TaskOptions.changelog`, and the Worker pushes Changelog records into the same `#outputs` array as sink outputs, in write order. One `producer.send` per batch carries both, before the offset commit — the exactly-once transaction (ticket 15) wraps that send unchanged. The Test Driver passes no `changelog`, so it logs nothing.
- A null/undefined key now throws when the store is logged (runtime only): a compacted topic rejects keyless records.
- Topics are created in `src/internal-topics.ts` (`ensureChangelogs`) before the Worker joins the group: `cleanup.policy=compact`, partitions = max partition count of the input topics (the Co-partitioning check is ticket 09's), replication = new config `changelogReplicationFactor` (default 3). An existing Changelog with another partition count fails `start()`; a create race with another instance is tolerated. Waits for partition leaders.
- Not done here: auto-create off / missing-topic listing (spec story 44), Repartition Topics, window-retention Tombstones.
