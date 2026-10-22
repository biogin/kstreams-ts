# 20: Crash suite covers rebalances

**What to build:** The crash test suite also scales Workers and processes up and down mid-run and kills during rebalances, still checking for exact results.

**Blocked by:** 17, 19, 29

**Status:** ready-for-agent

- [ ] Scenarios: scale up, scale down, kill during rebalance
- [ ] Same exact-comparison oracle as ticket 17

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 29

- Cover a SIGKILLed process whose Tasks move to a different process (the new owner waits out the dead process's transaction timeout), and taking over a `.lock` left behind by a dead process.
