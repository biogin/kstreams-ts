# 21: Event Time, Stream Time, timers

**What to build:** Processors can schedule timers on Stream Time or wall-clock time, Stream Time per Task only moves forward, and output records inherit the input record's Event Time. The Test Driver can advance wall-clock time.

**Blocked by:** 06

**Status:** ready-for-agent

- [ ] Stream Time per Task. `ctx.schedule` (Stream Time and wall-clock)
- [ ] Output timestamp = input Event Time
- [ ] Test Driver: pipe with timestamps, advance wall-clock time

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
