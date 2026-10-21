# 28: Abort on revoke, commit on graceful shutdown

**What to build:** When a rebalance revokes a Task mid-batch under exactly-once, the Worker aborts the Kafka transaction and rolls back SQLite (ADR-0006). A graceful shutdown commits the current batch, writes Checkpoints, closes stores and leaves the group, falling back to abort when the timeout runs out.

**Blocked by:** 16

**Status:** done

- [x] Revoke mid-batch → Kafka abort + SQLite rollback, verified by a test that forces a rebalance
- [x] Graceful shutdown commits within a configurable timeout
- [x] Timeout expiry falls back to abort and still leaves the group

Split from the original ticket 16.

## Implementation notes (from research)

- Rebalance events are notify-only (fits ADR-0006). The client does not drop records already buffered from revoked partitions, so filter them out.
- After a rebalance, commit offsets for kept partitions with the consumer's current group metadata, not the message's (see ticket 15 notes).
- Research notes: .scratch/mvp/research/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/28-revoke` (`ticket/28` already existed, as an old merge branch).
  - **API:** `StreamsAppConfig.shutdownTimeoutMs` (default 30000). `stop()` finishes and commits the current batch, closes the State Stores and leaves the group. If that takes longer, the batch is given up (exactly-once: Kafka abort, SQLite rollback; at-least-once: outputs dropped, offsets not committed), stores and Tasks are closed, and the consumer still leaves the group (forced close). `stop()` then resolves and the app is `stopped`.
  - **Ownership (`src/ownership.ts`):** follows `consumer:group:join` (notify-only) and records, for each owned `topic:partition`, the generation it was assigned in. A partition missing from a new assignment, or held again after a generation gap (a rebalance this member missed), is *lost*. Records are dropped if their partition isn't owned, or if they were fetched (message `metadata.consumer.generationId`) before the current ownership began. The eager assignor stays.
  - **Commit cycle (`src/worker.ts`):** a loss ends the batch (the record that noticed it is held for the next batch). At batch end, lost Tasks get no wall-clock timers. Exactly-once: if the batch has records of a lost partition, or the commit fails with a rebalance error (`needsRejoin`, or the ownership check before `addOffset`), it does a Kafka abort and SQLite rollback, then closes *every* Task (a rollback doesn't undo Stream Time, timers or Processor state) and drops `#held`. Then the Worker *rewinds*: it opens a second stream on the same consumer before closing the old one. That needs no rejoin, because the client ref-counts topics. So every owned partition is read again from its committed offset, and the Tasks are recreated with Restore. Otherwise the batch commits with the current group metadata. At-least-once: it commits offsets only for partitions it still owns; a commit that fails with a rebalance error is not fatal, and its offsets are retried with the next batch. Lost Tasks are closed after the commit.
  - **Duplicate deliveries:** after any rebalance the client rewinds the partitions it keeps to their committed offset. That offset can be stale, because `listCommittedOffsets` doesn't use `requireStable`. The cycle keeps the processed and the committed position of each partition and drops records below them.
  - **Tests:** `test/integration/rebalance.test.ts`:
    - (1) Exactly-once count with SQLite: 3 partitions, 3 s batches, continuous input. A raw consumer joins the group (eager revoke), then leaves. The app stays `running`, the committed outputs per key are exactly 1..n, read_uncommitted shows the aborted outputs, and SQLite holds the exact counts. This test failed before the change (the app went to `error`).
    - (2) At-least-once with two Streams App instances in one process, each with its own state dir. Tasks move to B and back with no lost update. Before the change a stale Task kept counting (e.g. 56 < 67).
    - (3) Exactly-once stop with a 120 s interval: the open batch commits at once, the stores hold the counts, and the group is empty.
    - (4) 1 s shutdown timeout with a Processor that hangs forever: stop resolves, no transaction is left open (LSO = end), nothing is committed, and the group is empty even with a 45 s session timeout. A restart processes all 3 records once.
    - `lifecycle.test.ts` validates `shutdownTimeoutMs`.
  - **Concerns for later tickets:**
    - Ticket 29 (several exactly-once members): the client's stream reads committed offsets without `requireStable`. A new owner can therefore start from a stale offset while the previous owner's transaction is still completing, which gives duplicates. Fix: before processing newly assigned partitions, fetch their stable offsets (`admin.listConsumerGroupOffsets({ requireStable: true })`) and seed the position map with them. Transactional IDs are also still per Worker number, so two exactly-once processes fence each other. That's why the exactly-once test uses a raw consumer as the second member.
    - Cooperative-sticky (29): Ownership already handles incremental assignments, but the client emits `group:join` only after the second round. Revoked partitions show earlier, in `consumer.assignments` and `group:rebalance`. Re-check the "continuous generation" rule there too, since a cooperative rebalance can raise the generation twice.
    - At-least-once: `consumer.commit` rejoins and retries on rebalance errors by itself, so it can commit a partition that was just revoked (this only causes duplicates).
    - A rebalance error during abort recovery repeats the abort and rewind until the rebalance settles. Each round closes all Tasks; Restore from the Checkpoint is cheap. After a stop gives up, the hung Processor's promise stays pending, and its later store writes hit closed stores.
    - Ticket 25: there is no `rebalancing` Application State and there are no events yet.
