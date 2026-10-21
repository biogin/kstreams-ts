# One transactional producer per Worker, not per Task

Each Worker has a single transactional producer covering all its Tasks, and each commit includes the offsets of every Task in it. Fencing comes from the consumer-group generation and member ID sent with each offset commit (KIP-447, confirmed in ADR-0004), not from a transactional ID per Task. That means fewer connections and simpler transactional IDs.

## Consequences

- One failing Task aborts the whole batch for every Task in that Worker. That's acceptable: a failure is either temporary and retried, or a bug that should stop the app anyway (fail-fast, ADR-0010).
