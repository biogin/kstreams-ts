# 28: Abort on revoke, commit on graceful shutdown

**What to build:** When a rebalance revokes a Task mid-batch under exactly-once, the Worker aborts the Kafka transaction and rolls back SQLite (ADR-0006). A graceful shutdown commits the current batch, writes Checkpoints, closes stores and leaves the group, falling back to abort when the timeout runs out.

**Blocked by:** 16

**Status:** ready-for-agent

- [ ] Revoke mid-batch → Kafka abort + SQLite rollback, verified by a test that forces a rebalance
- [ ] Graceful shutdown commits within a configurable timeout
- [ ] Timeout expiry falls back to abort and still leaves the group

Split from the original ticket 16.

## Implementation notes (from research)

- Rebalance events are notify-only (fits ADR-0006). The client does not drop records already buffered from revoked partitions, so filter them out.
- After a rebalance, commit offsets for kept partitions with the consumer's current group metadata, not the message's (see ticket 15 notes).
- Research notes: /Users/igorshalygin/.claude/jobs/e36bc646/tmp/notes/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
