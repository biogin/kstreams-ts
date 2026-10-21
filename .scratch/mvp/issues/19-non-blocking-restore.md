# 19: Restore without blocking other Tasks

**What to build:** Each Worker restores through a separate restore consumer outside the group, reading with `read_committed`. Only the Task being restored is paused, and the Worker's other Tasks keep processing.

**Blocked by:** 14, 18

**Status:** ready-for-agent

- [ ] Per-Worker restore consumer
- [ ] Per-Task pause/resume during Restore
- [ ] Integration test: a large store restoring does not stall another Task

## Implementation notes (from research)

- `@platformatic/kafka` has no per-partition pause, `assign()` or `seek()`. Pausing only the restoring Task (ADR-0009) is done by holding that Task's incoming records in memory until its Restore completes (bounded; document the memory trade-off).
- The restore consumer can use the low-level `consumer.fetch()` without a group, and it skips aborted transactions. Loop until the read_committed end offset, because transaction markers take up offsets (tested).
- Research notes: .scratch/mvp/research/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 18

- Shared `stateDir` across Workers with no per-Task directory lock (see ticket 29 note); non-blocking Restore must not open a Task's stores before the previous owner has closed them.
