# 20: Crash suite covers rebalances

**What to build:** The crash test suite also scales Workers and processes up and down mid-run and kills during rebalances, still checking for exact results.

**Blocked by:** 17, 19, 29

**Status:** ready-for-agent

- [ ] Scenarios: scale up, scale down, kill during rebalance
- [ ] Same exact-comparison oracle as ticket 17

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
