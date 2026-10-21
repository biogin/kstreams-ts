# Each Worker is its own consumer-group member

Every worker thread runs its own consumer, producer and State Stores, and joins the consumer group like any other process. Kafka does the load balancing, so we have no dispatcher in the main thread, no records copied between threads, and scaling up within a process works the same as scaling out across processes. With `workers: 0`, everything runs in the main thread, for tests, debugging and small apps.

## Considered Options

- The main thread consumes and dispatches records to workers: rejected. It copies every record between threads and needs our own scheduler, and transactions would have to cover several threads.
