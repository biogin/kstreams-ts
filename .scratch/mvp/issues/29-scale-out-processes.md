# 29: Scale out across processes

**What to build:** Running several processes with the same Application ID spreads Tasks across all their Workers using cooperative-sticky assignment, so rebalances move as few Tasks as possible.

**Blocked by:** 18

**Status:** ready-for-agent

- [ ] Cooperative-sticky assignor configured
- [ ] Integration test: 2 processes × 2 Workers share Tasks, results are exact under exactly-once
- [ ] Adding or removing a process moves only the Tasks it has to

Split from the original ticket 18.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 15

- Transactional IDs are `<applicationId>-worker-<n>`, per Worker number, not per process: two processes running the same exactly-once app would fence each other. Needs a stable instance identity (e.g. persisted in the state directory) folded into the transactional ID.
