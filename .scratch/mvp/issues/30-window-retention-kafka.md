# 30: Window retention and windows on Kafka

**What to build:** Window state is kept for window size plus Grace Period, then dropped and tombstoned in the Changelog. Windowed counts work end to end on real Kafka, including after Restore.

**Blocked by:** 22, 27

**Status:** ready-for-agent

- [ ] Retention = window size + Grace Period, then entries are dropped and tombstoned
- [ ] Integration test: windowed `count()` on Kafka, including after a restart

Split from the original ticket 22.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
