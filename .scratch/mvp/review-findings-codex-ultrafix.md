Reviewed `git diff 802bf92..HEAD`, focusing on `src/`. Findings below are based on source inspection. `npm test` could not start because Vitest’s temporary config write was denied by the read-only sandbox. No integration tests or broker operations were run.

## Per item

1. **Correct — Checkpoint invalidation.** Full Restore calls `clear()` (`src/restore.ts:143`), whose Checkpoint wrapper invalidates the file before clearing, including under exactly-once (`src/checkpoint.ts:229`).

2. **Incomplete — Store-key round-trip.** Writes now use the same deserialize-then-encode path as Restore (`src/state-store.ts:211`, `src/restore.ts:129`). Existing persistent stores remain incompatible because `CHECKPOINT_VERSION` is unchanged (`src/checkpoint.ts:51`). See finding 2.

3. **Introduces bug — PID-namespace liveness.** Established `.alive` locks distinguish live holders across namespaces (`src/state-directory.ts:470`). However, creation exposes the file before locking it, allowing concurrent cleanup to unlink it (`src/state-directory.ts:147`, `174`). See finding 1.

4. **Correct — Async observer failures.** Listeners and metrics sinks attach rejection handlers to returned thenables, while retaining synchronous exception handling (`src/observability.ts:292`, `307`, `366`).

5. **Correct — Send splitting and offsets.** Both committers send chunks sequentially and merge each chunk’s acknowledged offsets using the highest next offset (`src/worker.ts:1498`, `1605`, `1842`). A single oversized record remains unsplittable, as documented.

6. **Incomplete — Transaction timeout.** Admission budgeting limits ordinary queued processing (`src/worker.ts:1297`), but commit and abort classify errors using elapsed time captured before asynchronous requests (`src/worker.ts:1703`, `1752`). See finding 3. Recovery retains the old producer ID/epoch when retaking the transactional ID (`src/worker.ts:1623`, `1660`); I found no demonstrated zombie-fencing bypass.

7. **Correct — Full-Restore wipe.** Restore uses store-level `clear()` instead of materialising all keys (`src/restore.ts:143`). SQLite performs `DELETE FROM entries` directly (`src/sqlite-store.ts:52`, `75`).

8. **Correct — Null/undefined store keys.** Write rejection is unconditional, so it also applies without a Changelog in the Test Driver (`src/state-store.ts:221`).

9. **Correct — Integer timestamps.** Forwarded records, error-handler outputs and explicit Test Driver input timestamps are validated (`src/task.ts:553`, `src/error-handlers.ts:183`, `src/test-driver.ts:213`).

## New findings

1. **P1 — Concurrent startup can unlink a live holder’s liveness file.**  
   **Evidence:** `src/state-directory.ts:147–156`, `174–180`, `224–225`.

   Process A creates its final `.alive` filename before obtaining the exclusive SQLite lock. Process B’s startup cleanup can read the unlocked file, conclude that its holder is dead, and unlink it. A can subsequently hold an unlinked database. Its locks then either lack a liveness token—preventing recovery across namespaces after A dies—or carry a token whose file is missing, allowing a live holder’s locks to be reclaimed.

   **Suggested fix:** Acquire the SQLite lock under a private filename, then atomically publish the locked file before publishing any process or Task lock. Add a test covering concurrent creation and cleanup. This is a source-derived race, not experimentally reproduced.

2. **P2 — Changed persistent key encoding does not invalidate old Checkpoints.**  
   **Evidence:** `src/state-store.ts:211–228`; `src/checkpoint.ts:51`.

   Upgrade a clean persistent store containing Date keys serialized through JSON. Its old entries use object-tagged store keys; new reads and deletes use string-tagged keys after the Serde round-trip. Because version-1 Checkpoints remain accepted, incremental Restore leaves old entries untouched. Reads miss existing state, aggregates can restart, and subsequent writes create duplicate logical keys.

   **Suggested fix:** Bump the Checkpoint format version to force full Restore, as ADR-0015 requires, and test migration from a clean pre-fix store. Current round-trip tests cover fresh stores only.

3. **P2 — Timeout errors arriving during commit or abort remain fatal.**  
   **Evidence:** `src/worker.ts:1703–1710`, `1752–1760`; deadline check at `1677`.

   With a 10-second timeout, commit can begin at 8.9 seconds and receive `INVALID_PRODUCER_EPOCH` after the broker aborts at or beyond 10 seconds. Classification still receives the frozen 8.9-second duration, so it treats the error as fatal instead of aborting and replaying. Abort has the same stale-duration problem. Offset-registration awaits can also consume the commit margin without another deadline check.

   **Suggested fix:** Preserve the transaction’s opening timestamp and calculate elapsed time when handling the error. Recheck the deadline after offset registration and immediately before commit, while retaining epoch-checked recovery to preserve zombie fencing.

## Summary

Six fixes appear correct; three need follow-up for liveness publication, persistent-key migration and transaction-timeout classification.