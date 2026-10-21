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
