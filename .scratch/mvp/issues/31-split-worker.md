# 31: Split src/worker.ts

**What to build:** `src/worker.ts` (about 1,560 lines) has grown across many tickets and changes for unrelated reasons (Divergent Change, found in the code review): starting Tasks in the background, the commit cycle, how a batch commits under each Delivery Guarantee, and reporting state, events and metrics. Split it into modules along those lines, without changing behaviour.

**Blocked by:** none

**Status:** needs-triage

Suggested split (from the code review; to be confirmed in triage):

- [ ] **TaskStarter**: starting a Task in the background (`StartingTask`, `#start`, `#prepare`, `#takeStarted`, `#cancelStarts`, `#dispose`, the held-records bound, Task locks and stable offsets). The commit cycle asks it for started Tasks and tells it which partitions were lost.
- [ ] **WorkerReporter**: the Worker's state (`rebalancing`/`restoring`/`running`), events, metrics, Task counters, the consumer-lag sampler and the Restore listener.
- [ ] **Committers module** (e.g. `src/committers.ts`): `BatchCommitter`, `AtLeastOnceCommitter`, `TransactionalCommitter`, `CommitGuard`, `fencingOffsets`, `producedOffsets`/`mergeProduced`, `PartitionRevokedError`/`isRebalanceError`, `toMessage`.
- [ ] What stays in `worker.ts`: the commit cycle itself (reading records into batches, ownership and revokes, commit/abort, rewind, stop) and `startWorker`.
- [ ] Unit, integration and crash suites pass unchanged (integration tests don't import the Worker; see ADR-0016).

Deferred from the code-review fix pass (branch `fix/code-review`), which deliberately didn't split the file.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments
