# 13: Changelog + Restore on Kafka

**What to build:** Every State Store write goes to an auto-created, compacted Changelog (`<applicationId>-<store>-changelog`). After a SIGKILL and restart, `count()` results are correct because stores are fully restored from the Changelog.

**Blocked by:** 08, 12

**Status:** ready-for-agent

- [ ] Changelog topics auto-created: compacted, source partition count, replication factor from config (default 3, tests use 1)
- [ ] Full Restore from Changelog before a Task processes
- [ ] Deletes write tombstones to the Changelog
- [ ] Integration test: `count()` survives SIGKILL (at-least-once, so counts may over-count only by duplicates of uncommitted input; document the assertion precisely)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
