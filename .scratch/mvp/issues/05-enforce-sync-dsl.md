# 05: Enforcing synchronous DSL functions

**What to build:** A user who passes an async function to a DSL operator is stopped: at compile time with a message pointing to Processors, when the app is built for `async` functions, and on the first record for any function returning a thenable. See ADR-0012.

**Blocked by:** 03

**Status:** ready-for-agent

- [ ] Type-level test: async callback to `filter`/`map`/`peek` fails to compile with the named message
- [ ] Defining an app with an `AsyncFunction` throws when the app is built
- [ ] A plain function returning a Promise throws `AsyncNotAllowedError` on the first record. No unhandled rejection is left behind
- [ ] `AsyncNotAllowedError` is marked to skip error handlers (wiring verified in ticket 23)

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
