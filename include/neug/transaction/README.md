# Transactions

> Note: This file documents internal transaction implementation details. For
> application-facing behavior, see
> [Transaction Management](../../../doc/source/transaction/transaction.md).

## Read Transaction

With an `ReadTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

`ReadTransaction` provides a set of APIs to read the graph, including schema, topology, and properties.

After query with the `ReadTransaction` object, the transaction should be released by calling `ReadTransaction::Release()`.

## Insert Transaction

With an `InsertTransaction`, a set of vertices and edges can be inserted into the graph with the timestamp of transaction.

After insertion, the transaction can be committed by calling `InsertTransaction::Commit()` or be aborted by calling `InsertTransaction::Abort()`.

`InsertTransaction` does not provide interfaces to read the graph.

## Update Transaction

With an `UpdateTransaction`, a specific version of the graph can be read. The version is determined by the timestamp of the transaction.

Also, `UpdateTransaction` provides interfaces to insert and update vertices and edges.

After insertion and update, the transaction can be committed by calling `UpdateTransaction::Commit()` or be aborted by calling `UpdateTransaction::Abort()`.

`UpdateTransaction` mutates a copy-on-write `PropertyGraph` clone. Its changes are invisible until commit publishes the clone through `GraphSnapshotStore`.

# Version Management

## Visibility

Graph records that participate in MVCC visibility are associated with a timestamp, which is the timestamp of the transaction that creates or publishes the record version.

When reading graph data with a `ReadTransaction` or `UpdateTransaction`, only records with timestamp less than or equal to the transaction timestamp are visible.

## Synchronization

There is no synchronization between read and insert transactions in the normal state. All read and insert transactions can be executed concurrently.

The `VersionManager` packs the operation phase and active reader/inserter
counts into one atomic state word. It has four phases:

| Phase | Meaning | New Reads | New Inserts | New Writes | Existing Reads |
|-------|---------|-----------|-------------|-------------|----------------|
| `kNormal` | Normal | allowed | allowed | allowed | continue |
| `kUpdateExecution` | COW update execution | allowed | blocked | blocked | continue |
| `kPublishing` | COW snapshot publication | blocked | blocked | blocked | continue |
| `kExclusive` | In-place update or compaction | blocked | blocked | blocked | drained before mutation |

When an `UpdateTransaction` is created, the phase changes from `kNormal` to
`kUpdateExecution`. It waits for all in-flight insert transactions to finish,
but does not block or wait for read transactions. New inserts and writers are
blocked during this phase; existing and new reads continue on pinned
snapshots.

For COW commit, `VersionManager::begin_write_commit(..., kSnapshot)` changes
the phase to `kPublishing`, blocking new readers and inserts while
already-acquired readers continue on their pinned snapshots. In-place updates
use `kExclusive` and drain existing readers/inserters before mutation.

Read transactions acquire a `ReadSnapshotLease`, which loads the visibility
timestamp and view generation as one published value, pins a snapshot, and
retries unless the pinned generation matches. This prevents a reader from
combining an old visibility timestamp with a newer snapshot.

## Serializability

For a `ReadTransaction`, it will be assigned a graph timestamp. All insert or update transactions with timestamp less than or equal to that timestamp have been committed and are visible through timestamp filtering and the pinned snapshot.

For each `InsertTransaction` or `UpdateTransaction`, a unique timestamp will be assigned. When committing, a write-ahead log will be written to the disk and all modifications will be applied to the graph atomically.
