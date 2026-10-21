import { IS_CONTROL, MessagesStreamModes, Producer, stringSerializers } from '@platformatic/kafka'
import { afterAll, beforeAll, expect, it } from 'vitest'
import { ReadCommittedConsumer } from '../../src/read-committed.js'
import { brokers, createKafka, describeWithKafka, uniqueName } from './kafka.js'

// 2 KB values and a 1 KB fetch limit: one record batch per fetch response, so an aborted
// transaction's records and its abort marker come in different responses. The client's own
// read_committed filter delivers such records (platformatic/kafka#467).
const MAX_BYTES = 1024
const pad = (text: string) => text.padEnd(2048, '.')
const label = (value: Buffer | undefined) => value?.toString().replace(/\.+$/, '')

describeWithKafka('ReadCommittedConsumer', () => {
  let kafka: ReturnType<typeof createKafka>
  let topic: string
  let producer: Producer<string, string, string, string>

  beforeAll(async () => {
    kafka = createKafka()
    topic = await kafka.createTopic('read-committed')
    producer = new Producer({
      clientId: uniqueName('read-committed-producer'),
      bootstrapBrokers: brokers,
      idempotent: true,
      transactionalId: uniqueName('read-committed-txn'),
      producerId: -1n,
      producerEpoch: -1,
      serializers: stringSerializers,
    })
    const transaction = async (values: string[], end: 'commit' | 'abort' | 'open') => {
      const tx = await producer.beginTransaction()
      for (const value of values) {
        await tx.send({ messages: [{ topic, partition: 0, key: 'k', value: pad(value) }] })
      }
      if (end === 'commit') await tx.commit()
      if (end === 'abort') await tx.abort()
    }
    // Two aborted transactions of the same producer ID with a committed one between, then a
    // transaction left open: nothing after its start is readable (the last stable offset).
    await transaction(['aborted-1', 'aborted-2', 'aborted-3'], 'abort')
    await transaction(['committed-1'], 'commit')
    await transaction(['aborted-4', 'aborted-5'], 'abort')
    await transaction(['committed-2', 'committed-3'], 'commit')
    await transaction(['open-1'], 'open')
  })

  afterAll(async () => {
    await producer?.close(true).catch(() => {})
    await kafka.close()
  })

  it('fetch() returns only committed records, one batch per response', async () => {
    const consumer = new ReadCommittedConsumer({
      clientId: uniqueName('read-committed-fetch'),
      groupId: uniqueName('read-committed-fetch'),
      bootstrapBrokers: brokers,
    })
    try {
      const metadata = await consumer.metadata({ topics: [topic] })
      const found = metadata.topics.get(topic)
      const node = found?.partitions[0]?.leader ?? -1
      const received: string[] = []
      let fetchOffset = 0n
      let empty = 0
      while (empty < 3) {
        const response = await consumer.fetch({
          node,
          maxWaitTime: 200,
          topics: [
            {
              topicId: found?.id ?? '',
              partitions: [
                {
                  partition: 0,
                  fetchOffset,
                  currentLeaderEpoch: -1,
                  lastFetchedEpoch: -1,
                  partitionMaxBytes: MAX_BYTES,
                },
              ],
            },
          ],
        })
        const batches = response.responses[0]?.partitions[0]?.records ?? []
        if (batches.length === 0) empty++
        for (const batch of batches) {
          if (!(batch.attributes & IS_CONTROL)) {
            for (const record of batch.records)
              received.push(label(record.value ?? undefined) ?? '')
          }
          fetchOffset = batch.firstOffset + BigInt(batch.lastOffsetDelta) + 1n
        }
      }
      expect(received).toEqual(['committed-1', 'committed-2', 'committed-3'])
    } finally {
      await consumer.close(true)
    }
  })

  it('consume() delivers only committed records', async () => {
    const consumer = new ReadCommittedConsumer({
      clientId: uniqueName('read-committed-consume'),
      groupId: uniqueName('read-committed-consume'),
      bootstrapBrokers: brokers,
      maxBytes: MAX_BYTES,
      maxWaitTime: 200,
    })
    try {
      const stream = await consumer.consume({
        topics: [topic],
        mode: MessagesStreamModes.EARLIEST,
        autocommit: false,
      })
      const received: string[] = []
      const iterator = stream[Symbol.asyncIterator]()
      // Until nothing more comes: 1.5 s after the last committed record, at most 20 s.
      const wait = (ms: number) =>
        new Promise<'quiet'>((resolve) => setTimeout(() => resolve('quiet'), ms))
      let pending = iterator.next()
      for (;;) {
        const next = await Promise.race([pending, wait(received.length >= 3 ? 1_500 : 20_000)])
        if (next === 'quiet' || next.done) break
        received.push(label(next.value.value) ?? '')
        pending = iterator.next()
      }
      await stream.close()
      expect(received).toEqual(['committed-1', 'committed-2', 'committed-3'])
    } finally {
      await consumer.close(true)
    }
  })
})
