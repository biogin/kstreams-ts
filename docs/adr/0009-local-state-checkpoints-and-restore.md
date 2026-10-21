# Local State Stores persist across restarts with Checkpoints; unclean stores are wiped

SQLite State Store files live in a state directory and survive restarts. Each one records a Checkpoint, and Restore replays only the part of the Changelog after it. A store whose Checkpoint is missing or unclean is wiped and fully rebuilt. We never try to repair it, because the Changelog is the source of truth. Restore uses a separate per-Worker consumer outside the group, reading with `read_committed`, and only the Task being restored is paused. The Worker's other Tasks keep processing.

## Consequences

- v1 relies on cooperative-sticky assignment to keep Tasks where their local state is. A custom assignor that sends Tasks to Workers that already have their state (using per-member join metadata) is planned for v1.x.
- On disposable disks (e.g. Kubernetes without persistent volumes) every start is a full Restore. That works, but it's slow for large stores.
- `@platformatic/kafka` has no per-partition pause, seek or assign. "Only the Task being restored is paused" is implemented by holding that Task's incoming records in memory until its Restore completes, rather than pausing its partition at the consumer.
- "Unclean" means a store may hold a write its Changelog up to the Checkpoint doesn't have. A Task keeps one Checkpoint file for its persistent stores, writes it only after the Changelog records of every store write are acknowledged, and removes it before the next store write. So after a crash mid-batch the Checkpoint is missing, and the stores are wiped. A Checkpoint whose offset is outside the Changelog partition counts as unclean too. The file's format version is the on-disk format of ADR-0015.
