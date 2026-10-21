# 26: 0.1 release

**What to build:** The library is ready to publish as 0.1: user docs including the caveats (async Processors stretch transactions, disposable disks mean full Restore), CI running against Kafka 3.9 and 4.x, and the package no longer private.

**Blocked by:** 20, 23, 24, 25

**Status:** ready-for-agent

- [ ] README/user guide with examples for DSL, Processor API, Test Driver
- [ ] Documented caveats and the minimum broker version
- [ ] CI workflow for unit, integration (3.9 + 4.x) and nightly crash suite
- [ ] Remove `private`, version 0.1.0 (do not publish without the user)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
