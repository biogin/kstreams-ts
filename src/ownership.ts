// Internal: which input partitions a Worker owns, as its consumer-group membership changes.
//
// The Kafka client only tells us about rebalances after the fact, with events that can't make a
// rebalance wait (ADR-0006), and it neither drops records it already fetched for partitions that
// were revoked, nor rewinds a partition that was revoked and assigned again. So the Worker tracks
// for each owned partition the group generation it was assigned in, and only takes records fetched
// since then. A partition that was revoked, even if it came back, is "lost": its Task must be
// closed (and recreated, with a Restore) before anything else of it is processed.
//
// Under the cooperative protocol (task-assignor.ts) a rebalance that moves Tasks has two rounds,
// each a new generation. A member that gives Tasks up learns it in the first round, from
// `consumer:group:rebalance` with its assignment already reduced (the client sends no
// `consumer:group:join` for that round); every member gets `consumer:group:join` after the second.
// Both events update the ownership, so one rebalance raises the generation twice, by one each time.

/** What `Ownership` needs from the consumer. */
export interface GroupMember {
  readonly generationId: number
  readonly assignments: readonly { topic: string; partitions: number[] }[] | null
  on(event: 'consumer:group:join' | 'consumer:group:rebalance', listener: () => void): unknown
  off(event: 'consumer:group:join' | 'consumer:group:rebalance', listener: () => void): unknown
}

/** A record as fetched: its partition, and the generation the consumer was in at fetch time. */
export interface FetchedRecord {
  topic: string
  partition: number
  /** The client puts `consumer.generationId` here. */
  metadata: Record<string, unknown>
}

export class Ownership {
  readonly #member: GroupMember
  /** The generation of the last assignment seen. */
  #generation: number
  /** The generation each owned `topic:partition` was assigned in, and owned since. */
  #owned: Map<string, number>
  /** Partition numbers lost since the last `takeLost`. */
  #lost = new Set<number>()
  readonly #onLost: () => void
  readonly #onJoin = () => this.#assigned()
  readonly #onRebalance = () => this.#revoked()

  /** Starts from the member's current assignment; calls `onLost` when partitions are lost. */
  constructor(member: GroupMember, onLost: () => void) {
    this.#member = member
    this.#onLost = onLost
    this.#generation = member.generationId
    this.#owned = new Map()
    for (const key of assigned(member)) this.#owned.set(key, member.generationId)
    member.on('consumer:group:join', this.#onJoin)
    member.on('consumer:group:rebalance', this.#onRebalance)
  }

  /** Stops following the membership. */
  close(): void {
    this.#member.off('consumer:group:join', this.#onJoin)
    this.#member.off('consumer:group:rebalance', this.#onRebalance)
  }

  /** Whether `topic:partition` is owned now. */
  owns(topic: string, partition: number): boolean {
    return this.#owned.has(`${topic}:${partition}`)
  }

  /**
   * Whether `record` belongs to the current ownership of its partition: the partition is owned,
   * and the record was fetched after it was assigned (not left over from an earlier ownership).
   */
  current(record: FetchedRecord): boolean {
    const since = this.#owned.get(`${record.topic}:${record.partition}`)
    const consumer = record.metadata.consumer as { generationId?: unknown } | undefined
    const fetchedIn = consumer?.generationId
    return since !== undefined && typeof fetchedIn === 'number' && fetchedIn >= since
  }

  /** Whether partitions were lost since the last `takeLost`. */
  get lostPending(): boolean {
    return this.#lost.size > 0
  }

  /** The partition numbers lost since the last call. */
  takeLost(): ReadonlySet<number> {
    const lost = this.#lost
    this.#lost = new Set()
    return lost
  }

  /**
   * `consumer:group:rebalance`: either a rebalance is starting (nothing changed yet), or the first
   * round of a cooperative rebalance took partitions away (the assignment shrank).
   */
  #revoked(): void {
    const now = new Set(assigned(this.#member))
    if ([...this.#owned.keys()].every((key) => now.has(key))) return
    this.#assigned()
  }

  /** A completed rebalance round (or the client dropping the assignment): a new assignment. */
  #assigned(): void {
    const generation = this.#member.generationId
    // Each completed rebalance round a member takes part in raises the generation by one. A larger
    // step means this member missed one (e.g. it was dropped from the group), so it may have lost
    // every partition in between and owns none from before. The same generation: the client
    // dropped partitions without a rebalance; the others are still owned.
    const continuous = generation === this.#generation + 1 || generation === this.#generation
    this.#generation = generation
    const owned = new Map<string, number>()
    for (const key of assigned(this.#member)) {
      const since = continuous ? this.#owned.get(key) : undefined
      owned.set(key, since ?? generation)
    }
    let lost = false
    for (const [key, since] of this.#owned) {
      if (owned.get(key) !== since) {
        this.#lost.add(partitionOf(key))
        lost = true
      }
    }
    this.#owned = owned
    if (lost) this.#onLost()
  }
}

function assigned(member: GroupMember): string[] {
  return (member.assignments ?? []).flatMap(({ topic, partitions }) =>
    partitions.map((partition) => `${topic}:${partition}`),
  )
}

function partitionOf(key: string): number {
  return Number(key.slice(key.lastIndexOf(':') + 1))
}
