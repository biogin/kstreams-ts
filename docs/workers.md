# Worker threads, the App Module and scaling out

## Tasks and Workers

Input partitions are split into **Tasks**: one partition number across all (Co-partitioned) input topics. A Task owns its State Stores and is owned by exactly one **Worker** at a time. A Worker is a worker thread (or the main thread with `workers: 0`); each Worker is its own consumer-group member, with its own consumer, transactional producer and stores ([ADR-0007](./adr/0007-worker-is-group-member.md)). Tasks cannot be split further, so the useful number of Workers across all processes is at most the input partition count.

## Worker threads

`workers: 0` (the default) runs one Worker in the main thread: simplest for small apps and debugging. For more CPU use, `workers: { count, module }` runs `count` Workers in worker threads. Functions can't be handed to a worker thread, so each one **rebuilds the app from the App Module** ([ADR-0013](./adr/0013-workers-rebuild-app-from-app-module.md)).

## The App Module

The **App Module** is a module that exports the Streams App as its default export, **without side effects**: it must not call `start()` (or do anything else you don't want repeated in every thread, such as listening on a port). Name it in the app itself with `import.meta.url`, and start the app from another file:

```ts
import { createStreamsApp, serdes } from 'kstreams-ts'

// The App Module: every worker thread imports this file and rebuilds the app from its default
// export. It must have no side effects, so it must not call `start()`.
const app = createStreamsApp({
  applicationId: 'word-count',
  brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(','),
  stateDir: process.env.STATE_DIR ?? './state',
  guarantee: 'exactly-once',
  workers: { count: 4, module: import.meta.url },
})

app
  .stream('lines', { key: serdes.string, value: serdes.string })
  .groupByKey()
  .count({ store: 'counts' })
  .toStream()
  .to('counts', { key: serdes.string, value: serdes.number })

export default app
```

```ts
import app from './workers-app.js'

await app.start()
```

Things to know:

- The worker threads import the module as JavaScript. In a TypeScript project point `module` at compiled output (or run Node with type stripping, which Node 24 does by default for `.ts` files).
- Worker threads see the same environment variables and files as the main thread, so configuration must be derived from them, as above.
- The app a worker thread rebuilds must have the same Application ID; a different one, or a module without a default-exported Streams App, makes `start()` fail.
- The `app.on(...)` listeners, the `metrics` sink and the `state` live in the main thread: events and metrics of every Worker are passed to it. [Error handlers](./error-handlers.md) run in each worker thread.
- If a worker thread fails, the app goes to `error` and stops the other Workers; the error keeps its class (for the library's own errors and JavaScript's built-ins) and properties.

## Scaling out

To scale out, start more processes with the same Application ID and the same definition. Kafka's consumer group spreads the Tasks over all Workers of all processes, with cooperative-sticky assignment, so a rebalance moves as few Tasks as possible and they keep their local State Stores. When a Task moves, the new owner waits for the old one to close it, then restores whatever its local copy lacks (everything, if it never owned the Task before; see [operations](./operations.md#state-directory)).

- Each **App Process** keeps an App Process ID in its state directory, which makes its Workers' transactional IDs unique across processes and stable across restarts. Processes on one host may share a state directory; they lock Task directories while a Task is open.
- Give each process (container, host) its own state directory, kept across restarts if you can.
- Standby replicas and a state-aware assignor are not in 0.1, so a Task that moves to a Worker without its State Stores does a full Restore (see [known limitations](./limitations.md)).
