# Under exactly-once, a revoked Task aborts its open transaction

When a rebalance takes a Task away from a Worker mid-batch, the Worker aborts the open Kafka transaction and rolls back the matching SQLite transaction, without trying to commit first. The new owner reprocesses from the last committed offsets. This is correct by design and doesn't depend on a revoke hook that can make the rebalance wait, which `@platformatic/kafka` doesn't offer (ADR-0004). The cost is redoing at most one commit interval of work. Under at-least-once we commit on revoke when we can, and duplicates are acceptable.

## Consequences

- This applies only to revoke, which we don't control. Graceful shutdown, which we do control, finishes and commits the current batch, writes Checkpoints, closes stores and then leaves the group, with a shutdown timeout. If the timeout runs out, it falls back to abort.
