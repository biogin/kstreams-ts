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

## Notes from ticket 28

- The client reads committed offsets without `requireStable`. With several exactly-once members, a new owner can start from a stale offset and produce duplicates. Before processing newly assigned partitions, fetch their stable committed offsets.
- Transactional IDs are still per Worker number (see ticket 15 note), so two exactly-once processes fence each other; ticket 28's exactly-once rebalance test uses a raw consumer as the second member for that reason.
- Re-check `src/ownership.ts` under cooperative-sticky: join events can arrive late, and one rebalance can bump the generation twice.

## Notes from ticket 18

- Workers in one process share `stateDir` and nothing locks a Task's directory: a Task moving between Workers (or processes on one host) could be opened by its new owner before the old owner closes it. Add a per-Task lock (e.g. an exclusive lock file) and wait/retry on acquire.
- The stale-committed-offset risk (ticket 28 note) now also applies between Workers inside one process.
