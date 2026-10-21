# READ_COMMITTED consumer delivers records of an aborted transaction when the abort marker is in a later fetch response

## Summary

With `isolationLevel: READ_COMMITTED`, records of an **aborted** transaction are delivered to the application when the transaction's abort marker is not in the same fetch response as its records. This affects both `consumer.consume()` and the low-level `consumer.fetch()`, since both go through `#filterUncommittedMessages`.

This happens when an aborted transaction's records and its abort marker end up in different fetch responses: for example when the transaction is larger than one response (`maxBytes` / `maxBytesPerPartition`), or when the broker rolls a segment between the records and the marker. The Java consumer returns only the committed records for the same topic.

Related but different: #391 / #393 fixed a crash when a response contains an abort marker with no matching `abortedTransactions` entry. This issue is the opposite case: the response has the `abortedTransactions` entry but not the marker.

## Environment

- `@platformatic/kafka` 2.14.0 (the function below is unchanged on `main` at `cd6d19b`)
- Apache Kafka 4.4.0, single-node KRaft, `localhost:9092`
- Node.js v26.8.2, macOS

## Steps to reproduce

`repro.mjs` is included at the end of this issue (collapsed). The only dependency is `@platformatic/kafka@2.14.0`.

```sh
npm install
node repro.mjs                      # KAFKA_BROKER=host:port to override localhost:9092
```

The script:

1. Creates a topic with 1 partition.
2. A transactional producer writes `aborted-1`, `aborted-2`, `aborted-3` (2 KB values, one batch per send) and **aborts**. It then writes `committed-1` in a second transaction and **commits**.
   Offsets: 0–2 aborted data, 3 abort marker, 4 `committed-1`, 5 commit marker.
3. Reads the topic with `isolationLevel: READ_COMMITTED` and `maxBytes: 1024`. The broker then returns one batch per fetch response, so the abort marker (offset 3) arrives after the aborted records. It reads the topic twice: once with `consume()` and once with a `consumer.fetch()` loop.
4. Exits with code 1 if any `aborted-*` record is delivered.

`MAX_BYTES=1048576 node repro.mjs` is the control: everything fits in one response, and the output is correct.

## Expected

Only `committed-1` is delivered. The Java console consumer, run on a topic written by the same script (`KEEP_TOPIC=1`), with the same 1 KB fetch limits:

```
$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic> --from-beginning \
    --isolation-level read_committed \
    --consumer-property fetch.max.bytes=1024 --consumer-property max.partition.fetch.bytes=1024 \
    --formatter-property print.offset=true --timeout-ms 10000
Offset:4	committed-1
Processed a total of 1 messages
```

## Actual

Fails every time (3 runs out of 3):

```
topic read-committed-repro-1792583686185-innwst: wrote aborted-1..3 (ABORTED transaction), then committed-1 (COMMITTED transaction)
consume() read_committed received: 0:aborted-1, 1:aborted-2, 2:aborted-3, 4:committed-1
  FAIL: 3 record(s) from the aborted transaction were delivered
consumer.fetch() read_committed received: 0:aborted-1, 1:aborted-2, 2:aborted-3, 4:committed-1
  FAIL: 3 record(s) from the aborted transaction were delivered
```

Control run with `MAX_BYTES=1048576`:

```
consume() read_committed received: 4:committed-1
  OK: no aborted records delivered
consumer.fetch() read_committed received: 4:committed-1
  OK: no aborted records delivered
```

## Root cause

`Consumer#filterUncommittedMessages` in `src/clients/consumer/consumer.ts` ([v2.14.0, L2845–L2884](https://github.com/platformatic/kafka/blob/v2.14.0/src/clients/consumer/consumer.ts#L2845-L2884)). `fetch()` calls it for READ_COMMITTED ([L1120](https://github.com/platformatic/kafka/blob/v2.14.0/src/clients/consumer/consumer.ts#L1120)).

```ts
for (const aborted of abortedTransactions) {
  abortedRanges.set(aborted.producerId, [aborted.firstOffset, -1n])
}
for (const batch of partition.records!) {
  if (batch.attributes & IS_CONTROL && batch.records[0].key?.readInt16BE(2) === 0) {
    const abortedRange = abortedRanges.get(batch.producerId)
    if (abortedRange) {
      abortedRange[1] = batch.firstOffset
    }
  }
}
partition.records = partition.records!.filter(r => {
  const toSkip = abortedRanges.get(r.producerId)
  return !toSkip || !(toSkip[1] !== -1n && r.firstOffset >= toSkip[0] && r.firstOffset < toSkip[1])
})
```

The filter looks at each fetch response on its own. An aborted range is closed only by an abort marker in the same response. If the marker is not there, the range keeps `-1n` as its end, and every record in it is **kept**. The source comment describes this as intentional:

> For last offsets we set -1n as special value. It allows us to detect open ranges, whichs means
> that Kafka has not sent the control marker. In that case we ignore the range for safety.

For READ_COMMITTED, though, ignoring the range means *delivering* records that the broker has already reported as part of an aborted transaction, which is the unsafe direction. Nothing carries over to the next response. When the marker arrives later, the records it should have hidden have already been delivered.

In the repro, the first response holds only offset 0. Its `abortedTransactions` is `[{ producerId, firstOffset: 0 }]`, but the marker at offset 3 is not in it, so `aborted-1` is delivered. The same happens to offsets 1 and 2.

The broker's `abortedTransactions` list only says that a transaction from `producerId` that overlaps the fetched range was aborted, and where it starts. It is up to the client to skip that producer's transactional batches until it reads the matching abort marker, even when the marker comes in a later response. The Java client does this in `CompletedFetch` (`abortedProducerIds` / `abortedTransactions` priority queue, `isBatchAborted`, `consumeAbortedTransactionsUpTo`). A transaction whose `firstOffset` is at or below the current batch is treated as aborted. Its producer ID stays in the aborted set until that producer's abort marker is read, and once the marker is read the following batches are delivered.

## Impact

READ_COMMITTED does not guarantee that aborted data stays hidden. Any consumer that reads transactional topics can get records that were never committed: exactly-once pipelines, state restored from changelog topics, and so on. It depends only on how fetch responses happen to be split (batch sizes, `maxBytes`, segment boundaries), so it shows up rarely and can corrupt data downstream without any error.

## Suggested fix

Keep per-partition state across fetch responses, the way the Java client does:

1. For each assigned partition, keep the set of producer IDs whose aborted transaction has started but whose abort marker has not been read yet.
2. For each response, walk the batches in offset order. Before each batch, add the producer IDs of `abortedTransactions` entries with `firstOffset <= batch.lastOffset`. Entries with a `firstOffset` below the fetch offset belong to a transaction that is already in progress, so add them as well.
3. Drop a transactional data batch if its `producerId` is in the set. An abort marker removes its `producerId` from the set. Control batches are still passed through, so offset tracking keeps working as it does now.
4. Clear the partition's state when the fetch position is reset (seek, rebalance, offset out of range).

This also makes the open-range special case and the #391 guard unnecessary. I'm building a stream-processing library on top of `@platformatic/kafka` and already use a filter built this way around `consumer.fetch()`. It passes the scenario above (an aborted transaction spread over several 1 KB fetches, with committed and non-transactional records around it). I'm happy to open a PR based on it, with a test.

<details>
<summary><code>repro.mjs</code> and <code>package.json</code></summary>

```js
// Reproduction: a read_committed consumer delivers records of an ABORTED transaction
// when the abort marker is not in the same fetch response as the records.
//
// Usage: node repro.mjs            (broker from KAFKA_BROKER, default localhost:9092)
//        KEEP_TOPIC=1 node repro.mjs   (do not delete the topic, e.g. to compare with the Java consumer)
//        MAX_BYTES=1048576 node repro.mjs   (control: large fetch, marker in the same response -> passes)
import {
  Admin,
  Consumer,
  FetchIsolationLevels,
  MessagesStreamModes,
  Producer,
  stringDeserializers,
  stringSerializers
} from '@platformatic/kafka'

const brokers = [process.env.KAFKA_BROKER ?? 'localhost:9092']
const id = `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
const topic = `read-committed-repro-${id}`

// 2 KB values, one record batch per send. With a 1 KB fetch limit the broker returns
// one batch per fetch response, so the abort marker arrives in a later response.
const MAX_BYTES = Number(process.env.MAX_BYTES ?? 1024)
const pad = text => text.padEnd(2048, '.')
const label = value => value.replace(/\.+$/, '')

const admin = new Admin({ clientId: `repro-admin-${id}`, bootstrapBrokers: brokers })
await admin.createTopics({ topics: [topic], partitions: 1, replicas: 1 })

const producer = new Producer({
  clientId: `repro-producer-${id}`,
  bootstrapBrokers: brokers,
  idempotent: true,
  transactionalId: `repro-txn-${id}`,
  serializers: stringSerializers
})

async function transaction (values, commit) {
  const tx = await producer.beginTransaction()
  for (const value of values) {
    await tx.send({ messages: [{ topic, partition: 0, key: 'k', value: pad(value) }] })
  }
  await (commit ? tx.commit() : tx.abort())
}

await transaction(['aborted-1', 'aborted-2', 'aborted-3'], false)
await transaction(['committed-1'], true)
await producer.close()
console.log(`topic ${topic}: wrote aborted-1..3 (ABORTED transaction), then committed-1 (COMMITTED transaction)`)

const consumerOptions = {
  bootstrapBrokers: brokers,
  deserializers: stringDeserializers,
  maxBytes: MAX_BYTES,
  maxWaitTime: 500
}

// 1. High-level consume()
async function viaConsume () {
  const consumer = new Consumer({ ...consumerOptions, clientId: `repro-consume-${id}`, groupId: `repro-group-${id}` })
  const stream = await consumer.consume({
    topics: [topic],
    mode: MessagesStreamModes.EARLIEST,
    autocommit: false,
    isolationLevel: FetchIsolationLevels.READ_COMMITTED
  })
  const received = []
  const timeout = setTimeout(() => stream.close(), 15_000)
  for await (const message of stream) {
    received.push(`${message.offset}:${label(message.value)}`)
    if (label(message.value) === 'committed-1') break
  }
  clearTimeout(timeout)
  await stream.close()
  await consumer.close(true)
  return received
}

// 2. Low-level consumer.fetch()
async function viaFetch () {
  const consumer = new Consumer({ ...consumerOptions, clientId: `repro-fetch-${id}`, groupId: `repro-fetch-group-${id}` })
  const metadata = await consumer.metadata({ topics: [topic] })
  const { id: topicId, partitions } = metadata.topics.get(topic)
  const received = []
  let fetchOffset = 0n
  for (let i = 0; i < 50 && !received.some(r => r.endsWith(':committed-1')); i++) {
    const response = await consumer.fetch({
      node: partitions[0].leader,
      isolationLevel: FetchIsolationLevels.READ_COMMITTED,
      maxWaitTime: 500,
      topics: [{
        topicId,
        partitions: [{ partition: 0, fetchOffset, currentLeaderEpoch: -1, lastFetchedEpoch: -1, partitionMaxBytes: MAX_BYTES }]
      }]
    })
    for (const batch of response.responses[0].partitions[0].records ?? []) {
      const isControl = (batch.attributes & 0b100000) !== 0
      if (!isControl) {
        for (const record of batch.records) {
          received.push(`${batch.firstOffset + BigInt(record.offsetDelta)}:${label(record.value.toString())}`)
        }
      }
      fetchOffset = batch.firstOffset + BigInt(batch.lastOffsetDelta) + 1n
    }
  }
  await consumer.close(true)
  return received
}

let failed = false
for (const [name, run] of [['consume()', viaConsume], ['consumer.fetch()', viaFetch]]) {
  const received = await run()
  const leaked = received.filter(r => r.includes(':aborted-'))
  console.log(`${name} read_committed received: ${received.join(', ') || '(nothing)'}`)
  if (leaked.length) {
    failed = true
    console.log(`  FAIL: ${leaked.length} record(s) from the aborted transaction were delivered`)
  } else {
    console.log('  OK: no aborted records delivered')
  }
}

if (!process.env.KEEP_TOPIC) await admin.deleteTopics({ topics: [topic] })
await admin.close()
process.exit(failed ? 1 : 0)
```

```json
{
  "name": "platformatic-kafka-read-committed-repro",
  "private": true,
  "type": "module",
  "scripts": {
    "repro": "node repro.mjs"
  },
  "dependencies": {
    "@platformatic/kafka": "2.14.0"
  }
}
```

</details>
