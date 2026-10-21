# 24: Internal Topic checks

**What to build:** Existing Internal Topics with the wrong partition count are a fatal error. `autoCreateInternalTopics: false` checks the topics instead and lists exactly which ones are missing, and the replication factor is configurable.

**Blocked by:** 13

**Status:** ready-for-agent

- [x] Wrong partition count → fatal, actionable error
- [x] Auto-create off → check and list the missing topics
- [x] Replication factor option

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- Added `autoCreateInternalTopics` (default true). When false, `start()` fails with one error listing every missing Internal Topic with its partition count and `cleanup.policy=compact`; nothing is created. Wrong partition count on an existing Internal Topic is fatal in both modes, with one message style. Missing input topics are reported together in one error (replacing the client's raw "Unknown topic X."). `changelogReplicationFactor` is documented in the config type. Tests: test/integration/internal-topic-checks.test.ts; worker.ts only got the option passed through.
