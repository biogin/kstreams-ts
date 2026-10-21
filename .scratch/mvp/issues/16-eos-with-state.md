# 16: Exactly-once with state

**What to build:** Stateful apps under exactly-once keep the SQLite transaction open for the length of the Kafka transaction, with Changelog writes inside it, and commit SQLite plus the Checkpoint only after Kafka commits (ADR-0002).

**Blocked by:** 14, 15

**Status:** ready-for-agent

- [x] Commit cycle as in the spec Engine section: commit Kafka, then SQLite and Checkpoint
- [x] A Kafka abort rolls back the SQLite transaction
- [x] Integration test: `count()` is exact after SIGKILL + restart

Split from the original ticket 16; revoke and shutdown handling is ticket 28.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 27

- Restore currently runs as the Task's first queued job, inside the batch. Under exactly-once with stores, Restore must complete before the Kafka transaction begins (or outside any open transaction); otherwise a long Restore exceeds the transaction timeout.
- The consume() path still has the aborted-records client bug (abort marker in a later fetch response); Restore filters aborted transactions itself in src/restore.ts.

## Comments

- 2026-10-21: Done on branch `ticket/16`.
  - **Commit cycle (`src/worker.ts`, exactly-once):** each Task opens a store transaction on all its State Stores (SQLite: `BEGIN IMMEDIATE`, kept open across awaits) before its first record or timer of the batch → outputs and Changelog records go into the Kafka transaction as they are produced → last offset per partition added → Kafka commit → `task.commit()` (SQLite `COMMIT`) for every Task → `task.checkpoint(...)` with the Changelog offsets from the `transaction.send` results (base offset + records per partition, merged over the batch's sends, highest wins). Any failure: Kafka abort, then `task.rollback()` on every Task, then stores closed. `start()` no longer refuses exactly-once apps with State Stores.
  - **Task (`src/task.ts`):** new `TaskOptions.storeTransactions`, and `Task.restored()`, `commit()`, `rollback()`. `checkpoint()` throws while a store transaction is open.
  - **Restore before the transaction (ticket 27 note):** a record for a new Task (apps with stores, both guarantees) ends the batch it arrives in, unless it is the batch's first record; the next batch starts by awaiting that Task's Restore, before anything is processed, written or sent. So neither a Kafka nor a store transaction is open during a Restore, and Restore's per-chunk store transactions never nest. A long Restore still holds the whole Worker (ticket 19).
  - **Checkpoint under exactly-once (`src/checkpoint.ts`, ticket 14 concern 2):** `TaskCheckpoint` takes `changelogFirst`; then a store transaction's commit doesn't remove the Checkpoint (writes inside one never did). The store is then always its Changelog up to some offset at or after the Checkpoint, so a crash between the Kafka commit and the SQLite commit or the Checkpoint only replays that batch's Changelog records (absolute values, so harmless), with no wipe. `write()` now also writes when only the offsets advanced. ADR-0009's "unclean" bullet got a sentence on this.
  - **Fencing at startup:** under exactly-once the Worker begins and cancels (locally) a transaction before `consume()`: InitProducerId fences the previous producer, so the broker aborts (or completes) a transaction a crashed predecessor left open before the group's offsets are read and before any Restore. Without it, a zombie transaction holds the LSO (outputs and Changelogs) until its timeout.
  - **Tests:** `test/checkpoints.test.ts` › "Store transactions under exactly-once" (Restore before the first store transaction; rollback undoes writes since the last commit; Checkpoint kept through writes and commit, then advanced; no Checkpoint with an open store transaction; crash before the store commit / before the Checkpoint / after an abort all restore from the Checkpoint without a wipe, exact counts). Integration `test/integration/exactly-once-state.test.ts`: (1) a failing record after writes to an existing and a new key → app fails, SQLite on disk has only the first batch, the Changelog (read_committed, aborted transactions filtered) has only the first batch, the Checkpoint is still there; fails if store transactions are turned off. (2) `count()` with SQLite, 3 partitions, 4 SIGKILLs (each once the incarnation has committed progress, then 0–600 ms), input produced throughout: every key's committed outputs are exactly 1, 2, ... n, offsets cover all input, and after a graceful stop the SQLite stores hold the exact counts. Outputs are read with the restore reader (`createChangelogReader`), because the client's read_committed consumer can deliver aborted records (ticket 15 bug). `count-app` fixture takes `guarantee` and `stateDir`.
  - **Flake fix (`src/internal-topics.ts`):** under load, `createTopics` on a Changelog the broker's metadata didn't list yet failed with "Creating topics failed." (seen twice in full integration runs in `changelog.test.ts` › wrong partition count). The catch now polls `listTopics` for up to 2 s before rethrowing.
  - **Concerns for ticket 28:** `Task.rollback()` doesn't undo Stream Time, timers or Processor-local state, so a revoked/aborted Task must be closed and recreated (Restore from the Checkpoint is cheap now). A Task whose batch is held (`#held`) for Restore when a revoke comes needs dropping. Graceful stop already commits the current batch (store commit + Checkpoint); a stop during a long Restore waits for it. `#rollbackTasks` runs after an abort even while a Processor's async work is still in flight; its later writes hit a closed store.
  - **Concerns for ticket 17:** the SIGKILL test here is not randomized over the narrow windows (between Kafka commit and SQLite commit, between SQLite commit and Checkpoint); those are covered only by the unit tests. The crash suite should aim kills at them (e.g. many short batches), compare stores to a reference run, and read outputs with an abort-aware reader (not the client's consumer). Every batch begins and commits a SQLite transaction per Task even without writes; cheap but measurable.
