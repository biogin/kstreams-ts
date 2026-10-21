A correction to the "Root cause" and "Suggested fix" sections, after reading the Java client more carefully:

**The Java client does not keep state across fetch responses.** `CompletedFetch` is built for each partition of each response, and its `abortedProducerIds` / `abortedTransactions` come from that response only. Java gets this right because the broker lists every aborted transaction that overlaps the fetched range in each response, including transactions whose `firstOffset` is below the fetch offset. In the repro, the second response (fetch offset 1) lists `{ producerId, firstOffset: 0 }` again.

So the fix can be stateless, per response, like Java's:

1. Walk the response's batches in offset order, before anything is filtered out.
2. Before each batch, add the `producerId` of every `abortedTransactions` entry with `firstOffset <= batch.lastOffset`.
3. An abort marker removes its `producerId` from the set.
4. Drop a batch only if it is **transactional** (attributes bit `0x10`) and its `producerId` is in the set. Keep control batches in the output, since offset tracking relies on them.

This replaces steps 1 and 4 of my original suggestion: there is no per-partition state, so nothing needs resetting on seek, rebalance or offset reset. It also fixes two more problems in the current filter:

- `abortedRanges` is keyed by `producerId`, so two aborted transactions from the same producer in one response overwrite each other, and the second marker closes the wrong range.
- There is no transactional-flag check, so non-transactional batches from a producer ID with an aborted transaction could be dropped.

Also, a smaller correction to the summary: the reliable trigger is an aborted transaction whose records span more than one fetch response (e.g. larger than `maxBytes` / `maxBytesPerPartition`). Segment rolls on their own are not a reliable trigger.

The repro and the observed behaviour are unchanged.
