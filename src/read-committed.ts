// Internal: read_committed fetching that doesn't rely on the client's aborted-transaction filter.
//
// The client (@platformatic/kafka 2.14) filters aborted transactions out of read_committed fetch
// responses in `Consumer#filterUncommittedMessages`, and gets it wrong (platformatic/kafka#467):
// - it keys aborted ranges by producer ID, and every abort marker of that producer in the response
//   moves the range's end. A producer reuses its ID across incarnations (one transactional ID per
//   Worker, a bumped epoch each time), so a response can hold an aborted transaction, then
//   committed ones, then another abort marker of the same producer ID: the range then spans the
//   committed transactions and the first abort marker, and they are dropped;
// - an aborted transaction whose marker is not in the same response is not filtered at all;
// - it drops non-transactional batches of a producer ID with an aborted transaction.
//
// `ReadCommittedConsumer` sends every Fetch request with isolation level read_committed at the
// protocol level, so the broker bounds reads by the last stable offset and lists the aborted
// transactions, but tells the client's `fetch()` it reads read_uncommitted, so the client applies
// no filter of its own. `filterAbortedBatches` runs on every Fetch response instead, before the
// client (or `consume()`'s stream) sees it. This works for both the low-level `fetch()` and
// `consume()`, which fetches through the consumer's `fetch()`.
//
// Fetch sessions (KIP-227): the client keeps one incremental fetch session per broker node and
// consumer (`Consumer#fetchSessions`), but doesn't serialize the fetches that use it. Two
// concurrent `fetch()` calls to the same node read the same session ID and epoch, so the broker
// rejects the second with INVALID_FETCH_SESSION_EPOCH (71); each also sends only its own
// partitions and forgets the other's. The client then drops the session and retries, but the
// other fetches keep re-creating and advancing it, so the retries can fail the same way until
// `fetch failed N times`. A consumer that fetches concurrently (the restore consumer: one read
// per restoring Changelog partition) is created with `fetchSessions: false`: every Fetch request
// is then sessionless (session ID 0, epoch -1, a full fetch that creates no session), so the
// broker answers with session ID 0 and the client never caches a session. A consumer read by
// `consume()` keeps its sessions: a stream has at most one fetch in flight per node, and each
// stream has its own connection pool, which the client keys its cached session by.

import {
  type API,
  type Callback,
  type CallbackWithPromise,
  type Connection,
  Consumer,
  type ConsumerOptions,
  FetchIsolationLevels,
  type FetchOptions,
  type fetchV17,
  IS_CONTROL,
  IS_TRANSACTIONAL,
  kGetApi,
  type RecordsBatch,
} from '@platformatic/kafka'

/** An aborted transaction as a Fetch response lists it. */
export interface AbortedTransaction {
  producerId: bigint
  firstOffset: bigint
}

/** The control record type of an abort marker (the second int16 of a control record's key). */
const ABORT_MARKER = 0

/**
 * The batches of one partition of a read_committed Fetch response, with the records of aborted
 * transactions removed. This is the Java client's algorithm (`CompletedFetch`); it needs nothing
 * but the response itself:
 * - an aborted transaction listed in the response becomes "open" for its producer ID at the first
 *   batch whose last offset reaches the transaction's first offset (the broker lists every aborted
 *   transaction overlapping the fetched range, including those that started before it);
 * - an abort marker of that producer ID closes it;
 * - a transactional data batch of a producer ID with an open aborted transaction is aborted.
 *
 * An aborted batch is kept, with no records, rather than removed: readers advance their position
 * past the last batch of a response, which may be an aborted one. Control batches are kept too.
 * Batches are returned in the response's (offset) order; nothing is mutated.
 */
export function filterAbortedBatches(
  batches: readonly RecordsBatch[],
  abortedTransactions: readonly AbortedTransaction[] | null | undefined,
): RecordsBatch[] {
  if (abortedTransactions === null || abortedTransactions === undefined) return [...batches]
  if (abortedTransactions.length === 0) return [...batches]
  const pending = [...abortedTransactions].sort((a, b) =>
    a.firstOffset < b.firstOffset ? -1 : a.firstOffset > b.firstOffset ? 1 : 0,
  )
  let next = 0
  const aborting = new Set<bigint>()
  return batches.map((batch) => {
    const lastOffset = batch.firstOffset + BigInt(batch.lastOffsetDelta)
    for (; next < pending.length; next++) {
      const transaction = pending[next] as AbortedTransaction
      if (transaction.firstOffset > lastOffset) break
      aborting.add(transaction.producerId)
    }
    if (batch.attributes & IS_CONTROL) {
      if (isAbortMarker(batch)) aborting.delete(batch.producerId)
      return batch
    }
    if (batch.attributes & IS_TRANSACTIONAL && aborting.has(batch.producerId)) {
      return { ...batch, records: [] }
    }
    return batch
  })
}

/** Whether a control batch is an abort marker (rather than a commit marker). */
export function isAbortMarker(batch: RecordsBatch): boolean {
  const key = batch.records[0]?.key
  return key !== undefined && key !== null && key.length >= 4 && key.readInt16BE(2) === ABORT_MARKER
}

type FetchResponse = fetchV17.FetchResponse
type FetchApi = API<fetchV17.FetchRequest, FetchResponse>

/** Applies {@link filterAbortedBatches} to every partition of a Fetch response, in place. */
function filterResponse(response: FetchResponse | undefined): void {
  for (const topic of response?.responses ?? []) {
    for (const partition of topic.partitions) {
      if (partition.records) {
        partition.records = filterAbortedBatches(partition.records, partition.abortedTransactions)
      }
    }
  }
}

/** Marks the Fetch APIs wrapped by {@link readCommittedFetchApi}. */
const WRAPPED = Symbol('kstreams.readCommittedFetch')
const wrappedApis = new WeakMap<FetchApi, FetchApi>()
const sessionlessApis = new WeakMap<FetchApi, FetchApi>()

/** The session ID and epoch of a sessionless full Fetch request (KIP-227). */
const NO_SESSION_ID = 0
const FINAL_EPOCH = -1

/**
 * The client's Fetch API (any version), sending read_committed whatever isolation level it is
 * given and filtering the response with {@link filterAbortedBatches}. If `sessionless`, every
 * request is a sessionless full fetch, whatever session it is given (see the top of this file).
 */
function readCommittedFetchApi(api: FetchApi, sessionless: boolean): FetchApi {
  if (WRAPPED in api) return api
  const cache = sessionless ? sessionlessApis : wrappedApis
  let wrapped = cache.get(api)
  if (wrapped !== undefined) return wrapped
  const send = (connection: Connection, ...args: unknown[]): void => {
    const callback = (
      typeof args.at(-1) === 'function' ? args.pop() : () => {}
    ) as Callback<FetchResponse>
    // The request arguments: maxWaitMs, minBytes, maxBytes, isolationLevel, sessionId,
    // sessionEpoch, topics, forgottenTopicsData, rackId.
    args[3] = FetchIsolationLevels.READ_COMMITTED
    if (sessionless) {
      // The client never has a session then, so it already sends every partition and forgets none.
      args[4] = NO_SESSION_ID
      args[5] = FINAL_EPOCH
    }
    const request = args as unknown as fetchV17.FetchRequest
    api(connection, ...request, (error, response) => {
      try {
        filterResponse(
          response ?? (error as { response?: FetchResponse } | null)?.response ?? undefined,
        )
      } catch (filterError) {
        callback(filterError as Error)
        return
      }
      callback(error, response)
    })
  }
  wrapped = Object.assign(send, {
    async: (connection: Connection, ...request: fetchV17.FetchRequest) =>
      new Promise<FetchResponse>((resolve, reject) =>
        send(connection, ...request, (error: Error | null, response?: FetchResponse) =>
          error ? reject(error) : resolve(response as FetchResponse),
        ),
      ),
    key: api.key,
    version: api.version,
    [WRAPPED]: true,
  }) as FetchApi
  cache.set(api, wrapped)
  return wrapped
}

/** Options of a {@link ReadCommittedConsumer}: the client's, plus `fetchSessions`. */
export type ReadCommittedConsumerOptions<Key, Value, HeaderKey, HeaderValue> = ConsumerOptions<
  Key,
  Value,
  HeaderKey,
  HeaderValue
> & {
  /**
   * Whether Fetch requests use incremental fetch sessions (the default). Set it to false for a
   * consumer whose `fetch()` is called concurrently: see the top of this file.
   */
  fetchSessions?: boolean
}

/**
 * A consumer that reads read_committed with {@link filterAbortedBatches} instead of the client's
 * filter, through `fetch()` and `consume()` alike. Fetches are read_committed whatever isolation
 * level they ask for. Offset lookups (`listOffsets()`) are unchanged.
 */
export class ReadCommittedConsumer<
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer,
> extends Consumer<Key, Value, HeaderKey, HeaderValue> {
  readonly #sessionless: boolean

  constructor({
    fetchSessions = true,
    ...options
  }: ReadCommittedConsumerOptions<Key, Value, HeaderKey, HeaderValue>) {
    super(options)
    this.#sessionless = !fetchSessions
  }

  override fetch(
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback: CallbackWithPromise<FetchResponse>,
  ): void
  override fetch(options: FetchOptions<Key, Value, HeaderKey, HeaderValue>): Promise<FetchResponse>
  override fetch(
    options: FetchOptions<Key, Value, HeaderKey, HeaderValue>,
    callback?: CallbackWithPromise<FetchResponse>,
  ): Promise<FetchResponse> | undefined {
    // read_uncommitted as far as the client knows, so it doesn't filter; the protocol request is
    // read_committed (see `kGetApi`).
    const unfiltered = { ...options, isolationLevel: FetchIsolationLevels.READ_UNCOMMITTED }
    if (callback === undefined) return super.fetch(unfiltered)
    super.fetch(unfiltered, callback)
    return undefined
  }

  override [kGetApi]<RequestArguments extends Array<unknown>, ResponseType>(
    name: string,
    callback: Callback<API<RequestArguments, ResponseType>>,
  ): void {
    if (name !== 'Fetch') {
      super[kGetApi](name, callback)
      return
    }
    super[kGetApi]<RequestArguments, ResponseType>(name, (error, api) => {
      if (error !== null || api === undefined) {
        callback(error)
        return
      }
      const wrapped = readCommittedFetchApi(api as unknown as FetchApi, this.#sessionless)
      callback(null, wrapped as unknown as API<RequestArguments, ResponseType>)
    })
  }
}
