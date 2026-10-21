# 25: Observability

**What to build:** `app.state` reports the full Application State lifecycle (including rebalancing and restoring), typed events fire for state changes and errors, and a metrics interface reports lag, restore progress, commit duration, aborts, dropped late records and throughput.

**Blocked by:** 19, 30

**Status:** ready-for-agent

- [ ] Full Application State lifecycle
- [ ] Typed event emitter
- [ ] Metrics interface with the listed metrics

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
