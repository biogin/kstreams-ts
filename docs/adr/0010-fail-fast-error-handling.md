# Errors fail fast by default; produce errors can never be skipped

Deserialization and processing errors go to pluggable handlers. The default stops the application. A ready-made `deadLetter(topic)` handler is provided, and under exactly-once its dead-letter writes go in the same transaction. Produce errors (the broker rejects a write) always fail fast, because skipping them silently would break the Delivery Guarantee. We'd rather stop loudly than lose or duplicate data quietly.
