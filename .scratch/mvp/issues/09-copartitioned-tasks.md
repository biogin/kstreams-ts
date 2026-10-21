# 09: Co-partitioned Tasks

**What to build:** An app with several input topics forms Tasks from the same partition number across all of them, and refuses to start with a clear error if their partition counts differ.

**Blocked by:** 07

**Status:** ready-for-agent

- [ ] Task = partition number across all Co-partitioned inputs
- [ ] Startup check with an actionable error message on mismatch
- [ ] Integration test for both the happy path and a mismatch

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
