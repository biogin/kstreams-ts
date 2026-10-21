# 16: Exactly-once with state

**What to build:** Stateful apps under exactly-once keep the SQLite transaction open for the length of the Kafka transaction, with Changelog writes inside it, and commit SQLite plus the Checkpoint only after Kafka commits (ADR-0002).

**Blocked by:** 14, 15

**Status:** ready-for-agent

- [ ] Commit cycle as in the spec Engine section: commit Kafka, then SQLite and Checkpoint
- [ ] A Kafka abort rolls back the SQLite transaction
- [ ] Integration test: `count()` is exact after SIGKILL + restart

Split from the original ticket 16; revoke and shutdown handling is ticket 28.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
