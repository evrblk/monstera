# Core Implementation

This page is a practical, Go-level walkthrough of implementing an application core: from scoping and codegen to a
core running on a Monstera node. The high-level rules that every core must respect (determinism, no side effects,
error semantics, etc.) live in [Core Principles](/docs/core-principles.md) — this page assumes them and focuses on
*how* to satisfy them in code. All examples are taken from [Grackle](https://github.com/evrblk/grackle), a service
that provides distributed synchronization primitives.

The steps below are roughly in the order you would do them for a new core:

1. Scope the core and define shard key functions
2. Define the API: proto messages, `monstera.yaml`, codegen
3. Choose the storage model (`CoreType`)
4. Implement the `Core` struct and its tables
5. Implement update methods
6. Implement read methods
7. Implement `Snapshot`, `Restore`, and `Close`
8. Wire the core into a node
9. Drive time-based behavior from outside
10. Test

## Step 1: Scope the core and define shard keys

Before writing any code, decide what a **unit of work** is for this core — the piece of the domain whose requests
must always land on the same shard, wrapped in the same serializable transaction. This decision cannot be changed
later, so read [Units of Work](/docs/units-of-work.md) first. In Grackle a unit of work for `NamespacesCore` is an
account; for `LocksCore` it is a (account, namespace) pair.

Express the shard key as a small pure function in a dedicated package, and use it everywhere a shard key is needed
(request routing, restore-time bounds filtering, tests):

```go
package sharding

import "github.com/evrblk/monstera/utils"

func ByAccount(accountId uint64) []byte {
    return utils.GetTruncatedHash(utils.ConcatBytes(accountId), 4)
}

func ByAccountAndNamespace(accountId uint64, namespaceId uint64) []byte {
    return utils.GetTruncatedHash(utils.ConcatBytes(accountId, namespaceId), 4)
}
```

A 4-byte truncated hash gives a keyspace of `[0x00000000, 0xffffffff]` that shards are ranges of. The only hard
requirement is **stability**: `shardFunc(x)` must return the same bytes today and five years from now, or messages
will be routed to the wrong shard. Pin it with a golden test — a hardcoded list of inputs and expected outputs
(see [Testing](/docs/testing.md)).

## Step 2: Define the API and generate the boilerplate

A core's public contract is a set of **read methods** and **update methods**, each with a `*Request`/`*Response`
proto message pair. Define the messages in the `core_types_package` (Grackle: `pkg/corepb`), declare the methods in
`monstera.yaml`, and run codegen:

```shell
go tool github.com/evrblk/monstera/cmd/monstera code generate
```

This produces `api.go` (the `*CoreApi` interface you implement, plus request/response type aliases), `adapters.go`
(binary blobs → method calls), and `stubs.go` (typed client, clustered and nonclustered). See
[RPC Code Generation](/docs/rpc-code-generation.md) for the full reference. Two things are left for you to write by
hand:

* `ShardKey() []byte` on every sharded `*Request` payload — one line, calling your sharding package:

  ```go
  func (r *CreateNamespaceRequest) ShardKey() []byte {
      return sharding.ByAccount(r.NamespaceId.AccountId)
  }
  ```

* `MarshalBinary` / `UnmarshalBinary` on every payload type. These are one-line wrappers over the proto marshaller
  (Grackle generates them with a small `genmarshal` tool over `MarshalVT`/`UnmarshalVT`).

Classify methods carefully: anything that changes state is an update (goes through Raft, applied sequentially);
anything that only inspects state is a read (bypasses Raft, runs in parallel). Method numbers are wire identifiers
like proto field numbers — never renumber or reuse them. Old update messages live in Raft logs and snapshots and
will be replayed against future versions of your code.

The generated core interface methods look like this:

```go
CreateNamespace(req *coreapis.CreateNamespaceRequest) (*coreapis.CreateNamespaceResponse, error)
```

where the request wrapper carries the proto payload as `req.Payload` and the request timestamp as `req.Now` (Unix
nanoseconds), and the response wrapper carries `Payload`, an optional `ApplicationError`, and optional `Events`.

## Step 3: Choose the storage model

Every application declares a `CoreType`, and the framework derives all storage-dependent behavior from it (restore
on start, shard-split seeding):

* `CoreTypeInMemory` — state lives in RAM (plain structs, copy-on-write trees); the Raft log and snapshots *are*
  the durability. Fastest reads/updates; state must fit in memory; every restart is a snapshot restore + log replay.
* `CoreTypePersistedExclusive` — state lives in an embedded store (e.g. BadgerDB), with every
  row under a shard-unique key prefix. The physical store can be shared by all cores on the node, but no row
  is readable by more than one core. This is what all Grackle cores use.
* `CoreTypePersistedShared` — durable state keyed by shard-key range only, physically shared between cores with
  overlapping bounds. Cheapest splits, but the strictest key-layout discipline. This is similar to how CockroachDB
  stores data internally.

Start with `CoreTypePersistedExclusive` unless you know your state comfortably fits in RAM (then in-memory) — the
shard-unique prefix keeps cores independent, and the shared Badger instance keeps resource usage sane
(one database per node, many cores per process).

## Step 4: The Core struct and its tables

A core is a plain Go struct. It is constructed per replica by your factory function (Step 8) and receives everything
it needs explicitly — the shared store, its shard-unique prefix, and its shard bounds:

```go
type Core struct {
    badgerStore *store.BadgerStore

    shardPrefix     []byte // shard-unique key prefix (derived from shard id)
    shardLowerBound []byte // shard's key range, drives bounds-filtered Restore
    shardUpperBound []byte

    namespaces *namespacesTable
    counters   *countersTable
}

var _ coreapis.GrackleNamespacesCoreApi = &Core{} // compile-time check

func NewCore(badgerStore *store.BadgerStore, shardPrefix, lowerBound, upperBound []byte) *Core {
    return &Core{
        badgerStore:     badgerStore,
        shardPrefix:     shardPrefix,
        shardLowerBound: lowerBound,
        shardUpperBound: upperBound,
        namespaces:      newNamespacesTable(shardPrefix),
        counters:        newCountersTable(shardPrefix),
    }
}
```

Organize state into **tables** — small unexported types that own one entity's key layout, codec, and secondary
indexes. Business logic in core methods should speak in domain terms (`namespaces.GetByName`, `counters.Set`) and
never touch raw keys. A table derives its keys as `tableId + shardPrefix + primaryKey + sortKey` and maintains its
own indexes on every write:

```go
func (t *namespacesTable) Create(txn *store.Txn, namespace *corepb.Namespace) error {
    // secondary index: (account id, name) -> namespace id
    err := t.namesIndex.Set(txn, t.namesIndexPK(namespace.Id.AccountId, namespace.Name), namespace.Id.NamespaceId)
    if err != nil {
        return err
    }
    // primary table: (account id, namespace id) -> namespace
    return t.table.Set(txn, utils.ConcatBytes(t.tablePK(namespace.Id.AccountId), t.tableSK(namespace.Id.NamespaceId)), namespace)
}
```

DynamoDB-style thinking works well here: know your access patterns first, then design primary keys, sort keys, and
secondary indexes to serve them. Uniqueness constraints are just an index lookup before insert.

## Step 5: Update methods

An update method is one serializable transaction: open a write transaction, check invariants, mutate, commit.
Updates are applied on a single thread, so there are no locks and no race conditions to think about — the code reads
like single-user logic:

```go
func (c *Core) CreateNamespace(req *coreapis.CreateNamespaceRequest) (*coreapis.CreateNamespaceResponse, error) {
    txn := c.badgerStore.Update()
    defer txn.Discard()

    // Invariant: unique name per account
    _, err := c.namespaces.GetByName(txn, req.Payload.NamespaceId.AccountId, req.Payload.Name)
    if err != nil && !errors.Is(err, store.ErrNotFound) {
        return nil, err // internal error: kills the replica
    } else if err == nil {
        return &coreapis.CreateNamespaceResponse{ // application error: part of the response
            ApplicationError: mrpc.NewErrorWithContext(mrpc.AlreadyExists,
                "namespace with this name already exists",
                map[string]string{"namespace_name": req.Payload.Name}),
        }, nil
    }

    namespace := &corepb.Namespace{
        Id:        req.Payload.NamespaceId, // id generated by the caller, not here
        Name:      req.Payload.Name,
        CreatedAt: req.Now,                 // request timestamp, never time.Now()
        UpdatedAt: req.Now,
        Version:   1,
    }
    if err := c.namespaces.Create(txn, namespace); err != nil {
        return nil, err
    }

    if err := txn.Commit(); err != nil {
        return nil, err
    }
    return &coreapis.CreateNamespaceResponse{
        Payload: &corepb.CreateNamespaceResponse{Namespace: namespace},
    }, nil
}
```

The patterns to internalize:

* **Two error channels.** Domain outcomes (`NotFound`, `AlreadyExists`, `ResourceExhausted`, version mismatch) go
  into `ApplicationError` with a `nil` Go error — they are deterministic data, marshaled into the response and
  replayed identically. The Go `error` return is only for internal failures (disk full, corrupted state); returning
  it kills the replica (Principle 3). Never map a domain outcome onto the Go error.
* **Time comes from the request.** Read `req.Now` for every timestamp and expiry comparison. `time.Now()` inside a
  core produces divergent replicas (Principle 1).
* **Randomness comes from the caller.** Ids, tokens, and keys are generated outside the core and passed in the
  request (Principle 1). Since the core is the only place that can check uniqueness transactionally, handle the 
  (rare) collision as an application error — Grackle returns an internal `IDCollision` code that the front handler
  retries with a freshly generated id, never surfacing it to clients.
* **Use the transaction for invariants.** Uniqueness via index lookups, per-unit limits via counter rows updated in
  the same transaction, foreign-key-like checks by reading the parent row. This transactionality is the whole point
  of colocating a unit of work on one shard — use it.
* **Keep behavior stable across versions.** During a rolling deploy two code versions apply the same log entries
  (Principle 6). Gate any behavior change behind a flag stored in core state, defaulting to the old behavior, and
  flip it later with an explicit migration update.

Updates may also attach `Events` to the response — they are published to the Monstera pub/sub bus after the update
is applied and are the only sanctioned way for a core to "notify" the outside world.

## Step 6: Read methods

Reads run concurrently with updates and each other (Principle 4), so they must be genuinely read-only: open a
read-only transaction (a consistent MVCC view in Badger), never mutate state, and never cache anything mutable on
the struct:

```go
func (c *Core) GetNamespace(req *coreapis.GetNamespaceRequest) (*coreapis.GetNamespaceResponse, error) {
    txn := c.badgerStore.View()
    defer txn.Discard()

    namespace, err := c.namespaces.Get(txn, req.Payload.NamespaceId)
    ...
}
```

For in-memory cores the same rule means updates must swap state safely for concurrent readers (copy-on-write
structures or an `RWMutex` around a small state). List operations should be paginated with tokens — a unit of work
can hold millions of rows, and unbounded scans on the read path are how a healthy core becomes a slow one.

## Step 7: Snapshot, Restore, and Close

The full contract and its reasoning are in [Snapshot and Restore](/docs/snapshot-and-restore.md); the shape of an
implementation is:

* `Snapshot()` must return fast: pin a consistent view (a Badger read transaction, a copy-on-write root) and return
  an object whose `Write(w io.Writer)` streams it later, concurrently with new updates. `Release()` must drop the
  pinned view — leaking read transactions blocks Badger's garbage collection.
* `Restore(readers...)` has replace semantics: clear everything the core owns, then load the streamed entities that
  fall inside the core's shard bounds, rebuilding secondary indexes by inserting through the tables' normal write
  path. Snapshots are portable between cores of one application — this is what makes shard splits work — so stream
  only primary entities under canonical keys, with no storage prefixes.

Grackle packages all of this into a `tables` helper over the [fenestra](https://github.com/evrblk/fenestra) stream
format, so a core only declares its sections:

```go
func (c *Core) snapshotSections() []tables.Section {
    return []tables.Section{
        {Name: "Grackle.NamespacesCore.Namespaces", Table: c.namespaces},
        {Name: "Grackle.NamespacesCore.Counters", Table: c.counters},
    }
}

func (c *Core) Snapshot() monstera.ApplicationCoreSnapshot {
    return tables.NewSnapshot(c.badgerStore, "GrackleNamespaces", c.snapshotSections())
}

func (c *Core) Restore(readers ...io.ReadCloser) error {
    return tables.RestoreSnapshot(c.badgerStore, c.snapshotSections(),
        tables.ShardRange{Lower: c.shardLowerBound, Upper: c.shardUpperBound}, readers...)
}
```

Section names, like method numbers, are a wire compatibility contract: declare them explicitly and never rename
them.

`Close()` releases resources owned by this core instance. Do not close resources shared across cores — Grackle's
`Close` is empty because the Badger store belongs to the node, not the core.

## Step 8: Wire the core into a node

A node registers each application with an `ApplicationCoreDescriptor`: its `CoreType` plus a factory that builds a
core for a given shard and replica. The factory is where the shared store, the shard-unique prefix, and the bounds
come together, and where the generated adapter wraps your core:

```go
dataStore, err := store.NewBadgerStore(store.DefaultOptions(filepath.Join(dataDir, "cores")))
...

applicationDescriptors := monstera.ApplicationCoreDescriptors{
    "GrackleNamespaces": {
        CoreType: monstera.CoreTypePersistedExclusive,
        CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
            return coreapis.NewGrackleNamespacesCoreAdapter(
                replica.NodeId, shard.Id, replica.Id, shard.LowerBound, shard.UpperBound,
                namespaces.NewCore(dataStore, utils.GetTruncatedHash([]byte(shard.Id), 4), shard.LowerBound, shard.UpperBound))
        },
    },
}

monsteraNode, err := monstera.NewNode(dataDir, applicationDescriptors, monstera.DefaultMonsteraNodeConfig, transport)
```

The descriptor key must match `Application.Implementation` in the [cluster config](/docs/cluster-config.md). Call
`monstera.RegisterMetrics(...)` and the generated `coreapis.RegisterMetrics(...)` once at startup. Clients construct
the generated `*MonsteraStub` over a Monstera client; for local development and integration tests, the generated
nonclustered stub runs the same cores in one process with zero mocking (see
[Single Node Mode](/docs/single-node-mode.md)).

## Step 9: Drive time-based behavior from outside

Cores cannot use timers, TTLs, or background goroutines — nothing may happen except as the result of an applied
message. Expiration and garbage collection are therefore modeled as ordinary update methods, usually
`sharded: false` so a worker can target every shard explicitly, and invoked by a stateless external worker that
supplies the time:

```go
// external worker, NOT inside the core
shards, _ := client.ListShards("GrackleLocks")
for _, shardId := range shards {
    client.RunLocksGarbageCollection(ctx, &corepb.RunLocksGarbageCollectionRequest{
        GcRecordsPageSize: 100,
        MaxVisitedLocks:   1000,
    }, shardId)
}
```

Inside the core the GC update compares stored expiry timestamps against `req.Now` and deletes in bounded batches —
the request carries page-size limits because each invocation is one Raft-applied update and must not stall the
shard's write thread. The worker being external keeps the core deterministic: if the same GC message is replayed
next week, it deletes exactly the same rows.

## Step 10: Test

Cores are state machines with no side effects and no mocks — test them through their public API only, as sequential
scenarios: apply updates, verify with reads. An in-memory Badger store makes a real core cheap to construct per
test:

```go
func newNamespacesCore(t *testing.T) *Core {
    s, err := store.NewBadgerInMemoryStore()
    require.NoError(t, err)
    return NewCore(s, []byte{0x1d, 0x36, 0x00, 0x00}, []byte{0x00, 0x00, 0x00, 0x00}, []byte{0xff, 0xff, 0xff, 0xff})
}
```

Because time is an input, time-sensitive scenarios need no clock mocking — pass `now`, then `now.Add(time.Hour)`.
Beyond the domain logic, always cover:

* **Snapshot round-trips**: build state in core A, `Snapshot()`, apply more updates (proving the pinned view is
  isolated), `Write` to a buffer, `Restore` into core B, verify B matches the snapshot moment — including lookups
  through secondary indexes, which prove index rebuild works.
* **Shard key stability**: golden tests with hardcoded inputs and outputs.
* **Benchmarks**: a shard's write throughput is single-threaded core performance; aim for microseconds in-memory,
  sub-millisecond on disk.

See [Testing](/docs/testing.md) for the full testing strategy, including integration and load testing.

## Checklist

* Unit of work chosen deliberately; shard key functions pure, shared, and pinned by golden tests.
* One proto `*Request`/`*Response` pair per method; `monstera.yaml` method numbers never reused; codegen output
  compiles; `ShardKey()` and `MarshalBinary`/`UnmarshalBinary` implemented on payloads.
* `CoreType` declared; core constructed from shared store + shard prefix + bounds; state behind table types.
* Updates: one transaction each, application errors in the response, Go errors only for fatal internals, `req.Now`
  for time, ids from the caller, invariants enforced transactionally.
* Reads: read-only transactions, paginated lists, safe under concurrency with updates.
* `Snapshot()` fast and pinned; `Write` streams concurrently; `Release` always implemented; `Restore` clears,
  bounds-filters, and rebuilds indexes; `Close` touches only core-owned resources.
* Node registers descriptors with the right `CoreType`; metrics registered; nonclustered stub used for local dev
  and integration tests.
* No `time.Now()`, no `math/rand`, no TTLs, no goroutines, no external calls inside the core; GC driven by an
  external worker through explicit update methods.
