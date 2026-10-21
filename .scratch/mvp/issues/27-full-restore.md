# 27: Full Restore from the Changelog

**What to build:** Before a Task processes records, its State Stores are fully restored from their Changelog, so `count()` results are correct after a SIGKILL and restart.

**Blocked by:** 13

**Status:** ready-for-agent

- [ ] Full Restore from the Changelog before a Task processes
- [ ] Restored state includes deletes (tombstones)
- [ ] Integration test: `count()` survives SIGKILL under at-least-once (document the precise assertion: counts may only over-count by duplicates of uncommitted input)

Split from the original ticket 13.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Notes from ticket 15

- **Client bug (@platformatic/kafka 2.14.0):** a read_committed `consume()` delivers aborted records when the abort marker is not in the same fetch response (reproduced with old timestamps forcing a segment roll; Java consumer read 0). Check whether the low-level `fetch()` path used for Restore is affected. If it is, filter using the fetch response aborted-transaction lists yourself, tracked across responses, and test it with an aborted Changelog transaction spanning fetches.
