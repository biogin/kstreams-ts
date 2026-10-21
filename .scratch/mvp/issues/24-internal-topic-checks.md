# 24: Internal Topic checks

**What to build:** Existing Internal Topics with the wrong partition count are a fatal error. `autoCreateInternalTopics: false` checks the topics instead and lists exactly which ones are missing, and the replication factor is configurable.

**Blocked by:** 13

**Status:** ready-for-agent

- [ ] Wrong partition count → fatal, actionable error
- [ ] Auto-create off → check and list the missing topics
- [ ] Replication factor option

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
