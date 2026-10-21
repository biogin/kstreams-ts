# 13: Changelog writes and auto-created topics

**What to build:** Every State Store write (puts and deletes as tombstones) is recorded to an auto-created, compacted Changelog topic named `<applicationId>-<store>-changelog`, produced in the same commit cycle as the outputs.

**Blocked by:** 08, 12

**Status:** ready-for-agent

- [ ] Changelog topics auto-created: compacted, source partition count, replication factor from config (default 3, tests use 1)
- [ ] Every store put/delete is written to the Changelog partition matching the Task
- [ ] Integration test: after running `count()`, the Changelog contents match the final store contents

Split from the original ticket 13; Restore is ticket 27.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
