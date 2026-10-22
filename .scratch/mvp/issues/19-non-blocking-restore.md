# 19: Restore without blocking other Tasks

**What to build:** Each Worker restores through a separate restore consumer outside the group, reading with `read_committed`. Only the Task being restored is paused, and the Worker's other Tasks keep processing.

**Blocked by:** 14, 18

**Status:** done

- [x] Per-Worker restore consumer
- [x] Per-Task pause/resume during Restore (held in memory, bounded; see Comments)
- [x] Integration test: a large store restoring does not stall another Task

## Implementation notes (from research)

- `@platformatic/kafka` has no per-partition pause, `assign()` or `seek()`. Pausing only the restoring Task (ADR-0009) is done by holding that Task's incoming records in memory until its Restore completes (bounded; document the memory trade-off).
- The restore consumer can use the low-level `consumer.fetch()` without a group, and it skips aborted transactions. Loop until the read_committed end offset, because transaction markers take up offsets (tested).
- Research notes: .scratch/mvp/research/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 18

- Shared `stateDir` across Workers with no per-Task directory lock (see ticket 29 note); non-blocking Restore must not open a Task's stores before the previous owner has closed them.

## Notes from ticket 29

- Starting a Task now waits inside the commit cycle on three things: the per-Task `.lock`, stable committed offsets (src/stable-offsets.ts) and no open earlier transaction on the Changelog partition. Non-blocking Restore must move all three out of the commit cycle.
- An exactly-once abort still closes and restores every Task, including kept ones; consider restoring only what changed.

## Comments

- 2026-10-22: Done on branch `ticket/19`.
  - **Starting a Task in the background (`src/worker.ts`):** a record for a new Task that needs a start (State Stores, or exactly-once) creates a `StartingTask` and the cycle goes on. The start runs outside the commit cycle: wait for an earlier cancelled start of the same Task to wind down → Task directory `.lock` (only then are its stores opened) → stable committed offsets (exactly-once) → create the Task → its Restore (which first waits for the Changelog partition's LSO). All three waits from ticket 29 are off the cycle now. The cycle races its record reads against a "started" signal, and takes a started Task over at once (mid-batch is fine: the Task joins the batch like any other; its store transaction begins with its first record). It then raises `#position` to the stable offsets and processes the held records. A starting Task is never in `#tasks`, so it gets no `commit()`, `checkpoint()`, timers or offsets, and nothing of it is in the Worker's Kafka transaction; Restore's per-chunk store transactions never nest with a batch's. (The Worker's Kafka transaction is of course open for the *other* Tasks' batches meanwhile; that's the point.) The old "a record for a new Task ends the batch" rule and `#started` are gone.
  - **Memory bound:** all starting Tasks of a Worker together hold at most `commitMaxRecords` records (one batch's worth; default 1000). A starting Task that would go over it stops holding anything (it drops every later record, so held records never have gaps) and is marked overflowed; once started, the Worker rewinds its stream to the committed offsets (`#rewind`, as after an exactly-once abort) and `#position` drops what was already processed. So hitting the bound costs a re-fetch of up to one commit interval of the other partitions, never a stall of the other Tasks. Records redelivered by a rewind are not held twice (`heldUpTo` per topic-partition). Documented at the top of worker.ts, in ADR-0009's amendment bullet and on `commitMaxRecords`.
  - **Cancellation:** `restoreStores(..., signal)` and `ChangelogReader.read(..., signal)` take an `AbortSignal`: checked before each store, each chunk and each fetch, and the LSO/retry sleeps reject on abort. The lock and stable-offset waits give up on the same signal. A start is cancelled when its partitions are lost (`#forget`), on stop/failure (`run()`'s finally) and on `abandon()`. The cancelled Task is closed (no new Checkpoint) and its lock released only after its Restore has actually stopped; a later start of the same Task awaits that (`#cancelling`), since `lockTask` is reentrant for the same holder. `run()` awaits the winding-down starts before the Worker closes the restore consumer; `shutdownTimeoutMs` still bounds everything via `abandon()` + forced close.
  - **Exactly-once abort:** starting Tasks are kept (they had no part in the batch) and keep their held records. Started Tasks are still all closed and restored from their Checkpoints, now in the background. Avoiding that for kept Tasks would need snapshotting Stream Time, timers and Processor state (a rollback doesn't undo them), so it isn't cheap; not done.
  - **Tests:** unit `test/restore.test.ts` › "stops when cancelled". Integration `test/integration/non-blocking-restore.test.ts` (the restoring Task 1's Restore is held open by a foreign transaction left open on its Changelog partition, after 20,000 records written there): (1) exactly-once, `commitMaxRecords: 10`, 30 records for Task 1 (overflow → rewind, verified once with a debug print) while Task 0 commits 20 records one by one with a committed count after each; Task 1 has no new output meanwhile; after the gate commits, counts are exactly 1..21 and 1..31 (read_committed), and the SQLite store holds 20,002 entries with `b` = 31. Failed before the change (Task 0 stalled at its second count). (2) at-least-once stop during a Restore resolves in < 10 s with `shutdownTimeoutMs: 30_000`, group empty, Task 1's offset uncommitted. (3) exactly-once, two App Processes sharing one stateDir: the second takes the restoring Task (the highest, per task-assignor), the first cancels its Restore and releases the lock, the second restores it once the gate commits; counts exact (1..3, 1..11), both apps running.
  - **For later tickets:**
    - 20: kill/revoke during a background Restore is now a new window worth aiming at (a cancelled start must release the lock only after its last write; the overflow rewind under exactly-once). Cancellation takes effect between chunks; a chunk is applied in one store transaction.
    - 25: a `restoring` Application State / per-Task restore events have natural hooks in `#start`, `#takeStarted` and `#cancelStarts` (held count, overflow, lock/stable/LSO waits). Note the app is `running` while Tasks restore.
    - Many Tasks starting at once (startup) restore concurrently through one restore consumer; there is no limit on concurrent Restores.
