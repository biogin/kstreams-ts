# 15: Exactly-once tracer (stateless)

**What to build:** With `guarantee: 'exactly-once'`, a stateless app commits outputs and consumed offsets atomically using one transactional producer, adding only the last offset per partition per batch. A `read_committed` reader never sees aborted output.

**Blocked by:** 07

**Status:** done

- [x] Transactional producer per Worker (ADR-0008). Transactional ID derived from the Application ID
- [x] `addOffset` only for the last record per partition per batch
- [x] Integration test: forced abort leaves nothing visible to `read_committed`

## Implementation notes (from research)

- **Restart fencing bug:** a new Producer reusing a `transactionalId` gets `PRODUCER_FENCED` because the client sends producerId 0 / epoch 0. Pass `producerId: -1n, producerEpoch: -1` to the Producer constructor (verified: restart after a crash takes ~300 ms, zombie writes are rejected). Cover it in tests; consider reporting upstream.
- Committing or aborting an empty transaction fails with `INVALID_TXN_STATE`. Only begin a transaction when there is something to send or commit.
- `addOffset(message)` uses the generation the message was fetched under, so after any rebalance it fails with ILLEGAL_GENERATION. Pass the consumer's current group metadata instead (code in the note).
- Research notes: /Users/igorshalygin/.claude/jobs/e36bc646/tmp/notes/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- 2026-10-21: Done on branch `ticket/15`.
  - **API:** `guarantee: 'exactly-once'` is accepted. `start()` rejects an exactly-once app that has State Stores ("exactly-once with State Stores is not implemented yet") until ticket 16. Nothing else changed in the public API.
  - **Engine (`src/worker.ts`):** the commit step is now a `BatchCommitter`. `AtLeastOnceCommitter` keeps the old behavior: send, wait for acks, commit offsets. `TransactionalCommitter` sends outputs into the open transaction as they are produced, one send at a time to keep their order (outputs produced during a send go out together in the next). At batch end it adds the consumer, then one `addOffset` per partition for the last record read. These calls are sequential because the client merges concurrent offset commits into one. Then it commits. A transaction is only begun when there is something to send or commit. Any failure (Task, send, offset, commit) aborts the open transaction before the Worker closes. Transactional ID = `<applicationId>-worker-0` (same as the client ID). The producer is created with `producerId: -1n, producerEpoch: -1`, the restart fencing workaround, which the restart tests exercise.
  - **Offsets after rebalance:** `addOffset` uses the consumer's *current* group metadata, but only for partitions still in `consumer.assignments`. If a batch includes a partition that was revoked, the batch fails: abort, then error. Proper revoke handling is ticket 28.
  - **Transaction timeout:** the client sends its request `timeout` (default 5 s) as the transaction timeout. A batch longer than that was aborted by the broker and failed later with PRODUCER_FENCED. Under exactly-once the producer's `timeout` is now `commitIntervalMs + 10 s`.
  - **Fail-fast fix (both guarantees):** a failing Task (or send) now ends the batch right away. Before, the failure was only noticed when the commit interval ran out.
  - **Tests:** `test/integration/exactly-once.test.ts` covers several batches with exact output and committed offsets, tombstones and null keys, a forced abort, and a restart after a graceful stop with nothing processed twice. In the forced-abort test the outputs are visible to read_uncommitted but not to read_committed while the transaction is open, and stay invisible after the abort, with no offsets committed. A restart with the same Application ID then processes the batch exactly once. `kafka.consume` gained `uncommitted` and count 0 (read for `settleMs`), and `committedOffsets` uses `requireStable`.
  - **CLIENT BUG (@platformatic/kafka 2.14.0, read_committed):** the consumer only filters out an aborted transaction's records if its abort marker is in the *same* fetch response. Otherwise it treats the range as open and delivers the aborted records. The broker is fine: the Java console consumer read 0 records. Reproduced by producing in a transaction with old timestamps (2023): the broker rolls the segment, the marker lands in the next segment, and the aborted records reach read_committed consumers. Large aborted transactions that span fetch responses should hit the same bug. This breaks exactly-once for apps that read topics written by aborted transactions, including restore from Changelogs (tickets 16/19/27). Report it upstream and/or work around it. The tests use current timestamps to avoid it.
  - **For later tickets:** the transactional ID is stable per Worker number, not per process. Two processes running the same exactly-once app would fence each other (tickets 18/29 need a per-process or per-instance identity, ideally kept in the state directory so restarts still fence). Buffered records of revoked partitions are not dropped (ticket 28). No crash (SIGKILL) test yet (tickets 08/17). A restart after a crash with an open transaction is not tested yet.
