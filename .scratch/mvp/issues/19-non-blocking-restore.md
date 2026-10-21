# 19: Restore without blocking other Tasks

**What to build:** Each Worker restores through a separate restore consumer outside the group, reading with `read_committed`. Only the Task being restored is paused, and the Worker's other Tasks keep processing.

**Blocked by:** 14, 18

**Status:** ready-for-agent

- [ ] Per-Worker restore consumer
- [ ] Per-Task pause/resume during Restore
- [ ] Integration test: a large store restoring does not stall another Task

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
