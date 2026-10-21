# 14: Checkpoints and local persistence

**What to build:** SQLite files persist in `<stateDir>/<applicationId>/<task>/` across restarts. Restore replays only the part of the Changelog after the Checkpoint, and a store with a missing or unclean Checkpoint is wiped and rebuilt (ADR-0009).

**Blocked by:** 13

**Status:** ready-for-agent

- [ ] Checkpoint written with each commit
- [ ] Restart replays only after the Checkpoint (verify by counting records replayed)
- [ ] Missing or unclean Checkpoint → wipe and full Restore

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
