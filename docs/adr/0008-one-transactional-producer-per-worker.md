# One transactional producer per Worker, not per Task

Each Worker has a single transactional producer covering all its Tasks, and each commit includes the offsets of every Task in it. Fencing comes from the consumer-group generation and member ID sent with each offset commit (KIP-447, confirmed in ADR-0004), not from a transactional ID per Task. That means fewer connections and simpler transactional IDs.

## Consequences

- One failing Task aborts the whole batch for every Task in that Worker. That's acceptable: a failure is either temporary and retried, or a bug that should stop the app anyway (fail-fast, ADR-0010).
- Transactional IDs are `<applicationId>-<processId>-worker-<n>`. The App Process ID makes them unique across processes, and is kept in the state directory so a restarted process takes the same IDs and fences its old producers at once (see `src/state-directory.ts`). Without a state directory, each start gets a new ID, and a crashed process's open transaction is only aborted when it times out.
