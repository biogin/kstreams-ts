# 16: Exactly-once with state

**What to build:** Stateful apps under exactly-once keep the SQLite transaction open for the length of the Kafka transaction, with Changelog writes inside it, and commit SQLite plus the Checkpoint only after Kafka commits (ADR-0002).

**Blocked by:** 14, 15

**Status:** ready-for-agent

- [ ] Commit cycle as in the spec Engine section: commit Kafka, then SQLite and Checkpoint
- [ ] A Kafka abort rolls back the SQLite transaction
- [ ] Integration test: `count()` is exact after SIGKILL + restart

Split from the original ticket 16; revoke and shutdown handling is ticket 28.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 27

- Restore currently runs as the Task's first queued job, inside the batch. Under exactly-once with stores, Restore must complete before the Kafka transaction begins (or outside any open transaction); otherwise a long Restore exceeds the transaction timeout.
- The consume() path still has the aborted-records client bug (abort marker in a later fetch response); Restore filters aborted transactions itself in src/restore.ts.
