# 17: Crash test suite v1

**What to build:** A randomized crash test suite (ADR-0011) repeatedly SIGKILLs a single Worker running a stateful exactly-once app and checks that outputs and State Stores exactly match a reference run.

**Blocked by:** 08, 16

**Status:** ready-for-agent

- [ ] Randomized kill times, configurable iteration count
- [ ] Exact comparison: nothing missing, no duplicates, stores match
- [ ] Runnable as a separate npm script (nightly-style), documented in the README

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
