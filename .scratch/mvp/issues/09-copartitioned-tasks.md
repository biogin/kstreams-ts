# 09: Co-partitioned Tasks

**What to build:** An app with several input topics forms Tasks from the same partition number across all of them, and refuses to start with a clear error if their partition counts differ.

**Blocked by:** 07

**Status:** ready-for-agent

- [x] Task = partition number across all Co-partitioned inputs
- [x] Startup check with an actionable error message on mismatch
- [x] Integration test for both the happy path and a mismatch

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)

## Comments

- The Worker already keyed Tasks by partition number alone, so records from every input topic with the same partition number go to one Task. The new integration test proves it: a count fed by two topics sees each key's records from both.
- `src/co-partitioning.ts` checks partition counts with the Worker's consumer (`metadata`, forced refresh) before it joins the group, so a mismatch fails `start()` (state `error`) without taking any Task. The error lists every input topic's partition count.
- A missing input topic now fails `start()` too, with the client's "Unknown topic" error. Ticket 44's "list missing topics" work may want to fold that into one message.
- Internal Topics (Changelogs, tickets 43/45) and joined Tables can reuse the count `checkCoPartitioned` returns instead of querying again.
