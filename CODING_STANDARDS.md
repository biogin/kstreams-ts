# Coding standards

Rules for reviewing kstreams-ts changes. Each rule names a bug class that has already shipped in this codebase once; flag any diff that reintroduces it. Glossary terms (`CONTEXT.md`) and ADRs (`docs/adr/`) also apply. Formatting and types are tooling-enforced; skip them.

## Exactly-once and ownership

- **Fenced writes**: every Task that contributes anything to a transaction (records, outputs, Changelog writes, timer writes) also contributes an offset to it, so the group coordinator validates the commit.
- **Continuous ownership**: ownership is checked across every `await` up to the commit, never as a snapshot taken before asynchronous work.
- **Bounded transactions**: nothing inside an open transaction is unbounded in time; a batch must finish well inside the transaction timeout.
- **Rewind on regain**: a partition that is lost and regained restarts from its committed offset; records fetched under a previous ownership are never trusted.

## State

- **Changelog first**: a State Store changes only after every serialization the write needs has succeeded; a write that can't be logged fails the batch.
- **One key path**: store keys are derived identically at write time and at Restore.
- **Unclean means wipe**: any operation that invalidates a store's contents also invalidates its Checkpoint.
- **No whole-store loads**: wipes, scans and Restore never materialise a whole store in memory.

## Liveness and locks

- **No clock or PID liveness**: never judge a lock holder dead by wall-clock time or PID equality alone; use OS-held locks or identities unique across hosts and PID namespaces.

## Runtime parity

- **Same rules everywhere**: the Test Driver and the Kafka runtime validate input identically (keys, timestamps, Serdes). A rule enforced only in one runtime is a finding.

## Robustness

- **Bounded batches**: anything batched (produce requests, sends, deletes) has a byte or record bound.
- **User callbacks can't crash the process**: listeners, metrics sinks and handlers may be sync or async; a throw or a rejected promise is caught.
- **Errors keep their cause**: the first real error wins; errors caused by the app's own shutdown never replace it.

## Tests

- Tests drive public seams (ADR-0016) and assert behaviour, not internal call counts.
- A fix comes with a test that fails without it.
- Integration tests use condition-based waits and the abort-aware reader for exactly-once outputs.
