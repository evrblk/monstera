# Monstera Architecture (agent notes)

Dense orientation for working on deep features. Pair with `docs/` (user-facing). Verify specifics against 
code before relying on them.

## What Monstera is

A framework for stateful Go services: you write in-memory/embedded-DB state machines
("application cores"); Monstera handles Raft replication, sharding, snapshots. Cores are
FSMs behind hashicorp/raft. Updates are replicated then applied sequentially (serializable);
reads bypass Raft and hit the core directly (concurrent), optionally on followers.

## Request path (top → bottom)

app code → generated `<Stub>MonsteraStub` (`rpc/codegen`) → `monstera.Client` (`client.go`)
→ `transport.Transport` (`transport/grpc` or `transport/local`) → `Node` (`node.go`)
→ `replica` (`replica.go`) → `internal/raft.Raft` → hashicorp/raft → `fsmAdapter` →
`appCoreAdapter` → user `ApplicationCore` (`core.go`).

## Key files

- `client.go` — `Client`: shard-key routing, leader-first replica ordering (from cached
  `ReplicaState` via periodic `HealthCheck`), retry across replicas/nodes. Public API is
  shard-key based: `Read/Update(ctx, appName, shardKey, ...)`, `ReadShard/UpdateShard(...shardId...)`.
- `node.go` — `Node`: hosts replicas, `Read`/`Update` routing + leader forwarding, lifecycle
  (`INITIAL→READY→STOPPED`). `replicaForShard` resolves the local replica by shard key against
  the node's OWN config (correct under config-version skew), else by shard id.
- `replica.go` — `replica` wraps `raft.Raft` + `appCoreAdapter`. `coreMu` (RWMutex) guards
  core `Read` (RLock) vs `Restore` (Lock); `Apply`/`Snapshot` run on raft's single FSM thread.
- `core.go` — `ApplicationCore` interface: `Read/Update([]byte)`, `Snapshot`, `Restore`, `Close`.
  `ApplicationCoreDescriptor{CoreFactoryFunc, RestoreSnapshotOnStart}`.
- `cluster/config.go` — `Config`: apps→shards→replicas + nodes. `FindShardByShardKey`,
  `GetShard/GetReplica/GetNode`, `Validate`, `ValidateTransition`, builders (`CreateShard/Replica/...`),
  JSON round-trip with hex bounds (`ShortenBounds`, `MarshalJSON`). `KeyspacePerApplication = 1<<32`.
- `store/badger.go` — shared `BadgerStore` (one per node, all replicas). `BatchUpdate` = WriteBatch+Flush.
  `NewBadgerStore(DefaultOptions(dir).WithSyncWrites(bool))`.
- `internal/raft/` — `raft.go` (Raft wrapper: Apply timeout, leader lookup, snapshot streaming),
  `store.go` (`HraftBadgerStore` implements hraft `LogStore`+`StableStore` over shared store,
  keyed by `[]byte(replicaId)` prefix; caches first/last index), `transport.go` (adapts monstera
  transport → hraft transport, ships AppendEntries/Vote/InstallSnapshot over RPC), `codec.go`/`pbconv.go`.
- `rpc/` — generic wire envelope `Request{MethodNumber,Data,Now}` / `Response{Data,Error}`,
  `Error`/`ErrorCode`, `ErrorToGRPC`. `rpc/api.go` has generic wrappers `ReadRequest[T]`,
  `ReadResponse[T]`, `UpdateRequest[T]`, `Update/ReadUnshardedRequest[T]`.
- `rpc/codegen/` — codegen from `monstera.yaml`; `cmd/monstera` CLI (`code generate`, `config`).
- `transport/transport.go` — `Transport` interface + `ReadRequest`/`UpdateRequest` (no ReplicaId;
  carry ApplicationName, ShardId, ShardKey, Payload, Hops, AllowReadFromFollowers).

## Core invariants

- Keyspace per app = 4 bytes `[0x00000000, 0xffffffff]`, split into contiguous shards
  (inclusive bounds, no gaps/overlap; `Validate` enforces full coverage). Shard keys should be 4 bytes.
- A node hosts **≤1 replica per shard** (`Validate`: a shard's replicas are on distinct nodes).
  So `(node, shardId)` → unique replica; and future shard-split keeps `shardKey → active shard` unique.
- `Node.replicas` + `Node.clusterConfig` are a **matched pair** under `Node.mu` (RWMutex); they change
  only on config reload. Read/Update snapshot both under one RLock, then release before slow work.
- Cores must be **deterministic, side-effect-free, explode on internal errors**. No `time.Now()`/rand
  inside apply; the request timestamp is leader-stamped in `Request.Now` (Unix nanos). Watch map iteration order.
- Raft log store **must be durable**: `WithSyncWrites(true)`. `node.go` sets it for the raft store;
  `store.DefaultOptions` defaults to `false` (footgun for any new raft-store caller).

## Read/Update flow (node.go)

1. Client computes shard from key, orders candidate nodes leader-first, sends to a node.
2. Node `replicaForShard` → local replica. Follower-read or leader → serve locally.
3. Else forward to leader's node: `forward` request pinned by `ShardId=r.shardId`, `ShardKey` cleared
   (so the leader node doesn't re-resolve under a different config version), `Hops+1`, guarded by `MaxHops`.
   On `Unavailable`, `WaitForNewLeader` then retry once.
4. **Two intentional retry layers**: client (across nodes; handles unavailability + stale leader) and
   node (precise leader forwarding via live Raft state). The node layer is what you'd drop if switching to etcd/raft.

## RPC / codegen model

- `monstera.yaml`: `go_code.{output_package,core_types_package}`; `cores[].{read,update}_methods[].{name,
  method_number,sharded,allow_read_from_followers}`; `stubs[].{name,cores}`. Run
  `go tool github.com/evrblk/monstera/cmd/monstera code generate` in that dir. Generates:
  `api.go` (core+stub interfaces, per-method type aliases), `adapters.go` (dispatch by `method_number`,
  Prometheus metrics), `stubs.go` (`<Stub>MonsteraStub` + `<Stub>NonclusteredStub`).
- Per method `Foo`: needs `FooRequest`/`FooResponse` proto in `core_types_package`, each implementing
  `encoding.BinaryMarshaler`/`Unmarshaler`; sharded `*Request` implements `ShardKey() []byte`.
  These (MarshalBinary + ShardKey) are **user-provided**, NOT generated by monstera. Method numbers are 
  wire-stable; never renumber.
- Core method signature: `Foo(*FooRequest) (*FooResponse, error)` where the aliases wrap payloads in
  `rpc.ReadRequest[*corepb.FooRequest]` etc. Domain errors go in `Response.ApplicationError` (`rpc.Error`),
  not as Go errors; `nilifyIfEmpty` treats OK/INVALID codes as nil.

## Concurrency map

- `Node.mu` RWMutex → replicas + clusterConfig. `Node.smu` Mutex → nodeState.
- `appCoreAdapter.coreMu` RWMutex → core Read vs Restore.
- `HraftBadgerStore.mu` → first/last index cache (committed only after `BatchUpdate` success).
- `Client.mu` → replicaStates. `GrpcClientPool` mutex → conns. `raftMessageStream` pendingMu → correlation map.

## Open issues / not-yet-implemented

- Dynamic reconfiguration is incomplete: `Node.UpdateClusterConfig` swaps config but does NOT add/remove
  replicas (computes `replicasToAdd/Remove`, no-op), `DeleteConnection` is never called, `Client` has no
  config-refresh path. (H2)
- Events / Pub-Sub bus (`core.Event`, `UpdateResponse.Events`) is API-only, unimplemented.
- Updates are at-least-once (forward/retry can double-apply) — rely on core idempotency.
- Shard split (old active + two new shards mid-cutover) is planned; key-based resolution already anticipates it.

## Build & test

`make build` · `go test -race ./...` · `make lint`.
Fast inner loop: `go test -race ./rpc/... ./cluster/... ./store/... ./internal/raft/... ./utils/...`
(the `node_stop` integration test ~25s exercises failover/leadership).
