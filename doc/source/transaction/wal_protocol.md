# WAL protocol and upgrades

WAL framing is owned by the writer. A transaction accumulates logical redo
bytes and writes one frame before applying or publishing its changes. Empty
transactions do not write frames; compaction has an explicit empty frame.
The existing database WAL switch still controls whether a real or dummy writer
is used.

## Format version 1

A file contains a 24-byte file header followed by zero or more
`FrameHeader | payload` records. EOF terminates the file. Fixed metadata uses
explicit little-endian encoding, without C++ struct padding.

| Header | Fields, in order | Bytes |
|--------|------------------|------:|
| File | `NEUGWAL\0`, version u16, payload ABI u16, checkpoint ID u64, CRC32C u32 | 24 |
| Frame | kind u8, payload length u32, timestamp u32, header CRC32C u32, frame CRC32C u32 | 17 |

Version and payload ABI are both 1. The file CRC covers its first 20 bytes.
The header CRC covers the frame's first 9 bytes. The frame CRC covers those
same 9 bytes followed by the payload. There is no trailer or commit marker.
Kinds are Insert=0, CowRedo=1 and Compact=2. Only Compact has an empty payload.
Payload length is at most `2^30 - 1`; timestamps use the existing valid write
range, excluding zero and reserved timestamps.

Logical redo operation numbers and bytes are unchanged. ABI 1 requires the
current 64-bit little-endian platform ABI. Changes to redo field order,
operation numbers or type encoding require a protocol version change.

## Durability and recovery

Each execution slot owns a writer. Its first append exclusively creates a new
file; no preallocation or zero padding is used. Short writes and EINTR are
retried. A successful append includes file synchronization and synchronization
of directories needed to persist the new path. I/O or synchronization failure
after writing starts terminates the process. An apply or publication failure
after WAL success also terminates the process. Failed files are never repaired
or reused for further appends.

A complete valid frame can be recovered even if the client never received
success. Neither a checksum nor a marker proves synchronization completed or
that the client received an acknowledgement.

Recovery validates file headers and the independent frame header checksum
before trusting payload lengths. A partial final frame is ignored. A partial
file header is accepted only if it matches the expected header prefix. Empty
files and legacy preallocated files verified entirely zero with bounded reads
are accepted. Complete checksum failures, unsupported versions/ABIs/kinds,
invalid lengths/timestamps, epoch mismatches and duplicate timestamps reject
opening the database. Recovery does not search past corruption.

Files are mapped read-only and owned by the parser. Replay payload views expire
when the parser closes or is destroyed. Valid records are collected before
publication, sorted across slots by timestamp, and records already covered by
the checkpoint base timestamp are skipped. Timestamp gaps are allowed.
Consecutive Insert frames share a GraphView; CowRedo and Compact end that view.
Payload decoding is bounded, rejects invalid counts and flags, limits nesting
to 64, and must consume the entire payload. Decode or semantic replay failure
fails the entire open without publishing recovered state. Errors preserve the
WAL recovery exception with source path, offset and available timestamp.

## Upgrading existing databases

Use this order when a database has old WAL records:

1. Recover the database using its old executable.
2. Create a successful checkpoint and completely close that executable.
3. Open the database using the new executable.

The new executable rejects nonempty old WAL, including PR #833's experimental
format. It does not contain a second legacy replay implementation.
Checkpoint manifest v3 retains v2's data object layout. The new reader accepts
v2 and v3; a writable v2 open first verifies the old epoch contains no WAL
records, then publishes a v3 manifest and a new empty epoch using staging and
CURRENT. It reuses checkpoint objects, schema, scalars and base timestamp.
New WAL writing starts only after that publication succeeds. Read-only v2
opens do not upgrade. Old executables reject v3 before reading its new WAL.

The checkpoint ID detects an epoch mismatch. It does not identify a database
across copies; cross-database WAL copying is not supported. Compression,
fragmentation, encryption, group commit and automatic checkpoint policy are
outside this protocol change.
