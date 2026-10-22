# 32: Cheaper Checkpoints under at-least-once

**What to build:** At-least-once currently removes the Checkpoint on the first store write of each batch (unlink + directory fsync) and rewrites it on every commit (FULL WAL checkpoint per tracked store, temp-file write, fsync, rename, directory fsync) — about 4 fsyncs plus one FULL WAL checkpoint per store every 100 ms per Task. Reduce this: write Checkpoints only every N records or seconds, or on close/revoke, and flush only stores that changed since the last Checkpoint, while keeping the ADR-0009 guarantee that an unclean store is never trusted.

**Blocked by:** None (can start immediately)

**Status:** needs-triage

- [ ] Checkpoint frequency bounded by records/time (configurable), plus on close/revoke
- [ ] Only changed stores are flushed
- [ ] Crash suite still passes; a benchmark shows reduced commit latency

Source: ultrareview finding 10 (`.scratch/mvp/review-findings-ultrareview.md`).
