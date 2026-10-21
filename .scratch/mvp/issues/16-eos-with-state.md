# 16: Exactly-once with state

**What to build:** Stateful apps under exactly-once keep the SQLite transaction open for the length of the Kafka transaction (Changelog writes inside it), abort and roll back on revoke (ADR-0006), and commit the batch on graceful shutdown with a timeout.

**Blocked by:** 14, 15

**Status:** ready-for-agent

- [ ] Commit cycle as in the spec Engine section: commit Kafka, then SQLite and Checkpoint
- [ ] Revoke mid-batch → Kafka abort + SQLite rollback
- [ ] Graceful shutdown commits, falling back to abort when the timeout runs out
- [ ] Integration test: `count()` is exact after SIGKILL + restart

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
