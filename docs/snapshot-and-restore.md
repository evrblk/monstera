# Snapshot and Restore

Snapshots in application cores serve two main purposes:

* Raft log compaction
* Shard splits and child seeding

Log compaction is the classic Raft use: without snapshots the log would grow forever, and a node that fell far
behind (or a brand new replica joining a group) would have to replay the entire history. With snapshots, Raft
periodically captures the full state of the core, truncates the log behind it, and can bring a lagging replica up
to date by streaming the latest snapshot followed by the log tail.

Shard splits use the same machinery for a different job: a child shard is seeded from its **parent's** snapshot,
keeping only the rows that fall into the child's key range. This is why snapshots are required to be *portable*
between cores of one application (see below), not just a private round-trip of a single replica.

## When Snapshot and Restore are called

`Snapshot()` is called:

* Periodically by Raft (time- and log-size-based thresholds) for log compaction.
* On demand via the admin API (`TriggerSnapshot`).
* At the start of shard-split seeding, to produce the base state for the children.

`Restore(reader)` is called:

* On node start, for in-memory cores (`CoreTypeInMemory`): the core is rebuilt from the latest
  snapshot, and Raft replays the log tail after it. Persisted cores skip this — their on-disk state is already
  current, and only the snapshot *metadata* (its log index) is read.
* When a replica falls too far behind and the leader installs a snapshot on it over the network.
* During a shard split, on a freshly created child core, with the **parent's** snapshot as input.

Restore has *replace* semantics: the core must discard all of its current state first and load what the stream
contains. The framework guarantees Restore is never called concurrently with any other core method.

## Requirements

* **Write-once over `io.Writer`.** The producer gets a forward-only byte sink. No seeking, no back-patching, 
  no back-and-forth. The entire data set might not fit into memory, so it is designed to be iterated over and 
  written into the sink only once.
* **Single-pass reading.** The consumer reads the stream front to back exactly once. Nothing at the end of the 
  stream may be required to interpret the beginning (no footers, no trailing indexes).
* **Snapshotting is performed in two steps**. First, `Snapshot()` is called to take a snapshot of the current state. 
  Then, this snapshot is written to a sink. Snapshot creation is happening between FSM writes, but actual snapshot 
  writing to a sink happens in parallel with other FSM writes. Implementation of `Snapshot()` must be fast, typically 
  it is a copy operation for copy-on-write data structures, or a new transaction in database with `snapshot` 
  isolation level or higher.
* **Portable within an application.** A snapshot produced by any core of an application must be restorable by any
  other core of the same application. Keys inside the stream carry no storage prefixes and no shard identity;
  `Restore` keeps only the rows that belong to the restoring core's shard bounds and keys them under its own
  storage. For a same-shard restore every row is in bounds, so this costs nothing on the normal paths; for a
  shard split it is what lets a child consume its parent's snapshot directly.

## The interface

```go
// Returns a consistent view of the current state. Must return quickly; the
// expensive work belongs in Write. Called on the Raft FSM thread, never
// concurrently with Update.
Snapshot() ApplicationCoreSnapshot

type ApplicationCoreSnapshot interface {
    // Streams the state captured at Snapshot() time. Runs in parallel with
    // subsequent updates; the view must not be affected by them.
    Write(w io.Writer) error

    // Called when the framework is done with the snapshot. Release resources
    // captured at Snapshot() time (transactions, temp files).
    Release()
}

// Replaces the core state with the rows from the stream that belong to this
// core's shard bounds. Never called concurrently with any other method.
Restore(reader io.ReadCloser) error
```

Typical `Snapshot()` implementations of the "fast consistent view" contract:

* **Copy-on-write / immutable data structures**: capture the current root pointer.
* **BadgerDB (and other MVCC stores)**: open a read-only transaction; `Write` iterates it, `Release` discards it.
* **Plain maps**: copy the map under the same lock `Update` uses (acceptable when state is small; a large state
  wants a copy-on-write structure instead).
* **SQLite**: open a read transaction in WAL mode (or use the backup API); `Write` streams a consistent database
  image.

## Stream format

The framework never parses snapshot streams — the bytes are entirely the application's. The requirements above
(single pass, portability, bounds filtering) are the contract; how the bytes are framed is your choice.

The standard way to satisfy them is the `fenestra` library (`github.com/evrblk/fenestra`): a
framed, single-pass stream where rows are addressed as `(table id, canonical key)` — table ids are stable
application-wide names, keys carry no storage prefixes. Using it means the framing, truncation detection, and
canonical addressing problems are already solved, and generic tooling can inspect your snapshots. An application
is free to use its own encoding instead (for example, streaming a whole SQLite file is a perfectly valid
snapshot), as long as its `Restore` upholds the same contract.

## In-memory vs persisted cores

For **in-memory** cores (`CoreTypeInMemory`) the Raft log and snapshots *are* the durability story:
every restart is a `Restore` of the latest snapshot plus a log replay. Snapshot correctness is therefore exercised
on every node restart, not only in rare recovery scenarios — a subtle snapshot bug shows up as state divergence
after a restart.

For **persisted** cores (`CoreTypePersistedShared` / `CoreTypePersistedExclusive`) the core's own store is the 
durability story, and snapshots exist for the other consumers: follower catch-up, log compaction, and splits. 
`Restore` still runs at full fidelity when a snapshot is installed over the network — a persisted core cannot 
treat it as dead code.

## Practical notes

* Snapshots of one shard's replicas are *semantically* equal, not byte-identical — map iteration order and
  timestamps may differ. Nothing compares snapshot bytes; do not rely on byte equality, and do not pay extra to
  achieve it (sorting is optional).
* `Write` may run for a long time on large states; that is by design and does not block updates. What must be
  fast is `Snapshot()` itself.
* Always implement `Release()` — leaked read transactions block Badger's garbage collection; leaked temp files
  fill disks.
* A failed or truncated `Restore` must be safe to retry: do the cleanup (drop existing state) at the start, write
  in batches, and return errors instead of leaving half-state silently. The framework treats a Restore error as
  fatal for the replica and will retry recovery from scratch.
