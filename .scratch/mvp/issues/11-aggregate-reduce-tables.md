# 11: `aggregate`/`reduce`, Tables, deletes

**What to build:** A user aggregates or reduces per key into a Table, turns it back into a Stream and writes updates out. Deleting a key produces a tombstone.

**Blocked by:** 10

**Status:** ready-for-agent

- [ ] `aggregate`, `reduce`. Table → `toStream()`
- [ ] Store `delete` emits a tombstone downstream (and to the Changelog once ticket 13 lands)
- [ ] Tested through the Test Driver

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
