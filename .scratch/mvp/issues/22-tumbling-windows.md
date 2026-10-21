# 22: Tumbling windows in the Test Driver

**What to build:** A user writes `groupByKey().window(tumbling('1m', { grace: '10s' })).count()`. Late records beyond the Grace Period are dropped and counted, and window results carry the window end as their timestamp. Verified through the Test Driver.

**Blocked by:** 12, 21

**Status:** ready-for-agent

- [ ] Window store built on range scans
- [ ] Grace Period; late records dropped and counted
- [ ] Window end used as the result timestamp
- [ ] Test Driver tests

Split from the original ticket 22; retention and the Kafka integration test are ticket 30.

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
