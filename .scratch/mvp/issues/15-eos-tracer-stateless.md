# 15: Exactly-once tracer (stateless)

**What to build:** With `guarantee: 'exactly-once'`, a stateless app commits outputs and consumed offsets atomically using one transactional producer, adding only the last offset per partition per batch. A `read_committed` reader never sees aborted output.

**Blocked by:** 07

**Status:** ready-for-agent

- [ ] Transactional producer per Worker (ADR-0008). Transactional ID derived from the Application ID
- [ ] `addOffset` only for the last record per partition per batch
- [ ] Integration test: forced abort leaves nothing visible to `read_committed`

## Implementation notes (from research)

- **Restart fencing bug:** a new Producer reusing a `transactionalId` gets `PRODUCER_FENCED` because the client sends producerId 0 / epoch 0. Pass `producerId: -1n, producerEpoch: -1` to the Producer constructor (verified: restart after a crash takes ~300 ms, zombie writes are rejected). Cover it in tests; consider reporting upstream.
- Committing or aborting an empty transaction fails with `INVALID_TXN_STATE`. Only begin a transaction when there is something to send or commit.
- `addOffset(message)` uses the generation the message was fetched under, so after any rebalance it fails with ILLEGAL_GENERATION. Pass the consumer's current group metadata instead (code in the note).
- Research notes: /Users/igorshalygin/.claude/jobs/e36bc646/tmp/notes/kafka-client.md

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
