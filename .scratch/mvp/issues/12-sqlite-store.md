# 12: SQLite State Store

**What to build:** The SQLite State Store (`better-sqlite3`) passes the same contract suite as the in-memory store, plus order-preserving key encoding and range scans that windows will rely on.

**Blocked by:** 10

**Status:** ready-for-agent

- [ ] `better-sqlite3` store passes the contract suite
- [ ] Deterministic, order-preserving key byte encoding independent of the key Serde
- [ ] Range scans in key order (contract suite extended, in-memory store too)
- [ ] SQLite is the default durable store

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
