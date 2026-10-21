# State Store keys use an order-preserving, tagged encoding

State Store keys are not stored as the key Serde's bytes. They're encoded as a list of self-delimiting tagged parts whose byte order matches value order: null, booleans, numbers (8-byte, sign-flipped so negatives sort first), strings and bytes (zero-escaped, zero-terminated) and arrays. Keys of other types fall back to their Serde bytes. Composite keys such as `[key, windowStart]` support range and prefix scans, which windows need. Serde bytes don't sort by value (e.g. numbers), so range scans over them would be wrong.

## Consequences

- The encoding and the SQLite table layout are an on-disk format. Changing either needs a version bump that wipes local stores and restores them from the Changelog.
- Changelog records keep the key Serde's bytes, not this encoding. Restore re-encodes them, which only works if the key Serde round-trips exactly.
- Prefix scans run from the encoded prefix to its computed end, not by byte-prefix matching (`'a\0'` starts with the bytes of `'a'`).
