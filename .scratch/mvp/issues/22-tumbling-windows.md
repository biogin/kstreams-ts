# 22: Tumbling windows

**What to build:** A user writes `groupByKey().window(tumbling('1m', { grace: '10s' })).count()`. Late records beyond the Grace Period are dropped and counted, window results carry the window end as timestamp, and window state is retained for size plus grace before being dropped and tombstoned. Works in the Test Driver and against Kafka.

**Blocked by:** 12, 13, 21

**Status:** ready-for-agent

- [ ] Window store on range scans
- [ ] Grace Period, late drop counting
- [ ] Window end timestamps. Retention plus tombstones
- [ ] Test Driver tests plus one integration test

Spec: [../spec.md](../spec.md). Glossary: [CONTEXT.md](../../../CONTEXT.md). ADRs: [docs/adr/](../../../docs/adr/)
