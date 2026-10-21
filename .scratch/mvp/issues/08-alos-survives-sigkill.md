# 08: At-least-once survives SIGKILL

**What to build:** An app run as a child process can be SIGKILLed mid-stream and restarted, and every input record still shows up in the output. Duplicates are allowed. The child-process harness built here is reused by the crash test suite.

**Blocked by:** 07

**Status:** ready-for-agent

- [ ] Reusable harness: run a Streams App in a child process, SIGKILL at a chosen or random time, restart
- [ ] Integration test: kill mid-run, restart, assert nothing is missing

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
