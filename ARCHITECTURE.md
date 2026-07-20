# Monstera Architecture (agent notes)

Dense orientation for working on deep features. Pair with `docs/` (user-facing). Verify specifics against
code before relying on them. Design docs in `notes/` capture intent but drift; code wins.

## What Monstera is

A framework for stateful Go services: you write in-memory/embedded-DB state machines
("application cores"); Monstera handles Raft replication, sharding, snapshots. Cores are
FSMs behind hashicorp/raft. Updates are replicated then applied sequentially (serializable);
reads bypass Raft and hit the core directly (concurrent), optionally on followers.

## Two planes

- **Data plane** (`transport.DataPlane`) — hot path, **nodeId-addressed**, needs cluster config to
  resolve nodeId→address: `Read`, `Update`, `ListReplicaStates`, `RaftMessage`.
- **Admin plane** (`transport.AdminPlane`) — control path, **raw-address-addressed**, config-free
  (breaks the config chicken-and-egg for provisioning): `Bootstrap`, `Get/UpdateClusterConfig`,
  `ListReplicaStates`, `ListReplicaSnapshots`, `TriggerSnapshot`, `LeadershipTransfer`.
- Both are served by one gRPC service (`MonsteraApi`) on one listener; the split is client-side.

## Request path (top → bottom)

app code → generated `<Stub>MonsteraStub` (`rpc/codegen`) → `monstera.Client` (`client.go`)
→ `transport.DataPlane` (`transport/grpc` or `transport/local`) → `Node` (`node.go`)
→ `replica` (`replica.go`) → `internal/raft.Raft` → hashicorp/raft → `fsmAdapter` →
`appCoreAdapter` → user `ApplicationCore` (`core.go`).

Admin path: CLI / `control.Executor` → `transport.AdminPlane` (`transport/grpc/admin.go`) →
`Node.Bootstrap/UpdateClusterConfig/LeadershipTransfer/...`.

## Key files

- `client.go` — `Client`: shard-key routing, leader-first replica ordering (from `replicaStates`
  cached by a background loop polling `DataPlane.ListReplicaStates` per node), retry across
  replicas/nodes. `NewMonsteraClient(provider ClusterConfigProvider, trans transport.DataPlane, cfg
  ClientConfig)`; `Start(ctx)` wires `provider.Watch(onConfig)` — each newer config is swapped in and
  pushed into the data plane via `transport.ClusterConfigConsumer.SetClusterConfig`. Public API is
  shard-key based: `Read/Update(ctx, appName, shardKey, ...)`, `ReadShard/UpdateShard(...shardId...)`.
- `config_provider.go` — `ClusterConfigProvider` (`Latest`, `Watch`, `Start`, `Stop`).
  `StaticClusterConfigProvider`; `PollingClusterConfigProvider(discovery, admin, opts)` polls
  `AdminPlane.GetClusterConfig` on candidate addresses (discovery ∪ nodes of held config) and adopts
  the **highest Version** (monotonic → correct mid-rollout). Used by clients, not by `Node`.
- `node_discovery.go` — `NodeDiscovery.Endpoints(ctx)`: seed addresses to ask for config ("who to
  ask", vs provider's "what they say"). `Static`, `File` (one host:port per line), `SRV` (DNS).
- `node.go` — `Node`: hosts replicas, `Read`/`Update` routing + leader forwarding, lifecycle +
  provisioning + reconciliation (see below). `replicaForShard` resolves the local replica by shard key
  against the node's OWN config (correct under config-version skew), else by shard id.
- `replica.go` — `replica` wraps `raft.Raft` + `appCoreAdapter`; membership passthroughs
  (`AddVoter/RemoveServer/GetConfiguration/Bootstrap/LeadershipTransfer`). `coreMu` (RWMutex) guards
  core `Read` (RLock) vs `Restore` (Lock); `Apply`/`Snapshot` run on raft's single FSM thread.
- `core.go` — `ApplicationCore` interface: `Read/Update([]byte)`, `Snapshot`, `Restore`, `Close`.
  `ApplicationCoreDescriptor{CoreFactoryFunc, RestoreSnapshotOnStart}`.
- `cluster/config.go` — `Config`: apps→shards→replicas + nodes. `FindShardByShardKey` (binary search;
  `sortShards` invariant established on load), `GetShard/GetReplica/GetNode`, `Validate`,
  `ValidateTransition`, builders (`CreateShard/Replica/Node/Application`, `AddReplica` with
  caller-supplied id), `IncrementVersion`, `Hash()` (SHA-256 of proto), JSON round-trip with hex bounds
  (`ShortenBounds`, `MarshalJSON`). `WriteConfigToFile` is **atomic** (temp+fsync+rename).
  `KeyspacePerApplication = 1<<32`.
- `control/` — declarative reconfiguration sequences (see below): `sequence.go` (types + JSON
  load/save), `planner.go` (`PlanAddNode`, `PlanMoveShard`), `fold.go` (re-derive per-step configs from
  pinned base), `executor.go`, `gates.go`.
- `store/badger.go` — shared `BadgerStore` (one per node, all replicas). `BatchUpdate` = WriteBatch+Flush.
  `NewBadgerStore(DefaultOptions(dir).WithSyncWrites(bool))`.
- `internal/raft/` — `raft.go` (Raft wrapper: Apply timeout, leader lookup (`GetRaftLeader`,
  `WaitForNewLeader`), `GetRaftStats() RaftStats` (term/log/commit/applied indexes, LastContact),
  membership (`Bootstrap/AddVoter/RemoveServer/GetConfiguration`), snapshot install streaming via
  `snapshotSession` + `io.Pipe`), `store.go` (`HraftBadgerStore` implements hraft `LogStore`+`StableStore`
  over shared store, keyed by `[]byte(replicaId)` prefix; caches first/last index), `transport.go`
  (adapts `transport.DataPlane` → hraft transport; AppendEntries pipeline with inflight cap 20),
  `codec.go`/`pbconv.go`, `metrics.go` (Raft-layer Prometheus metrics + `Collectors()`).
  **Fully encapsulates `hashicorp/raft`:** nothing outside this package imports it. Cross-boundary types
  are monstera-owned (`RaftServer`, `SnapshotMetadata`, `RaftStats`, `RaftState`, `AppCore`/
  `AppCoreSnapshot`), so swapping the Raft lib is contained here — keep it that way.
- `rpc/` — generic wire envelope `Request{MethodNumber,Data,Now}` / `Response{Data,Error}`,
  `Error`/`ErrorCode`, `ErrorToGRPC`. `rpc/api.go` has generic wrappers `ReadRequest[T]`,
  `ReadResponse[T]`, `UpdateRequest[T]`, `Update/ReadUnshardedRequest[T]`.
- `rpc/codegen/` — codegen from `monstera.yaml`.
- `cmd/monstera` — CLI: `code generate` (codegen; the old top-level `generate` command is gone) and
  `cluster {bootstrap-node, bootstrap-nodes, add-node, move-shard, get-config}` (`commands/cluster.go`,
  drives the admin plane / `control` sequences; `add-node`/`move-shard` are plan+execute fused, resumable
  via `--sequence` checkpoint file).
- `transport/transport.go` — `DataPlane` + `AdminPlane` interfaces (former single `Transport`),
  `ClusterConfigConsumer` (optional data-plane capability), and DTOs: `ReadRequest` carries
  ApplicationName, ShardId, ShardKey, Payload, Hops, AllowReadFromFollowers; `UpdateRequest` the same
  minus AllowReadFromFollowers; neither carries ReplicaId. `ReplicaState{ReplicaId, RaftState, Stats}`.
- `transport/grpc/` — `dataplane.go`: `DataPlaneClient` (implements DataPlane + ClusterConfigConsumer;
  holds config under `configMu`, conn pool, per-node persistent bidi `raftMessageStream` with
  message-id→channel correlation; `SetClusterConfig` drops conns/streams of removed nodes). `admin.go`:
  `AdminClient` (address-keyed pool, unary calls, no config). `server.go`: `GrpcServer` serves both
  planes; `RaftMessage` handler is the server side of the bidi stream. `pool.go`: generic
  `GrpcClientPool[T]`.
- `transport/local/` — `LocalTransport` implements **both** planes in-memory over registered `*Node`s;
  admin "address" == nodeId.

## Node lifecycle & provisioning (node.go)

- States: `INITIAL → {UNPROVISIONED | READY} → STOPPED`; `UNPROVISIONED → READY` via `Bootstrap`.
  `UNPROVISIONED` = started with no applied config; serves only admin Bootstrap + status.
- On-disk layout (see `docs/directory-layout.md`): `config/node.json` (identity `{node_id, version}`),
  `config/cluster.json` (applied config, atomically rewritten), `raft/` (one shared Badger for all
  replicas' log+stable store), `snapshots/<replicaId>/`, `cores/` (convention).
- `Bootstrap(ctx, nodeId, config)`: rejects a different nodeId once provisioned; same/empty id is a
  no-op success (does NOT downgrade applied config). Write order matters: `cluster.json` first, then
  `node.json` as the **commit marker** — a crash in between leaves the node re-bootstrappable; an
  orphan `cluster.json` without identity is ignored on start.
- Identity is immutable once set; `NewNode` reads it and loads the applied config (must exist if
  identity does).

## Dynamic reconfiguration (implemented)

- `Node.UpdateClusterConfig(ctx, new)`: `Validate` → under `mu`: `ValidateTransition(old,new)` →
  atomic persist → swap → `reconcileReplicasLocked` → `bootstrapShards` → unlock →
  `refreshTransportConfig` → `reconcileRaftMembership` → version gauge.
- `reconcileReplicasLocked` (holds `mu`): creates replicas newly assigned to this node (NOT
  raft-bootstrapped — they join when the leader adds them as voter); removes unassigned ones:
  `Close` + `raftStore.DropPrefix(replicaId)` + delete snapshot dir. Delete-last is safe because
  replica ids are never reused.
- `bootstrapShards` (holds `mu`): only the shard's FIRST replica's node bootstraps the Raft group
  (deterministic), and only if not already bootstrapped.
- `reconcileRaftMembership` (no `mu` during RPCs): on shards this node **leads**, diffs desired member
  set vs `GetConfiguration()`; `AddVoter` missing, `RemoveServer` extra — but never removes self
  (leadership is transferred away first by the control plane). Idempotent; also runs on a background
  ticker (`startReconciler`, `MembershipReconcileInterval`, default 1s), so membership converges even
  without a config push.
- `ValidateTransition` invariants: version strictly increases; no shard removal/re-bounding; no
  replica node reassignment; no add+remove of replicas in the same transition; no removing a node that
  still has replicas.

## Control plane: sequences (control/)

- Model: a planner pins a base config (`BaseVersion` + `BaseHash`) and emits a `Sequence` of `Step`s,
  each a small individually-safe list of `Mutation`s (`add_node`/`add_replica`/`remove_replica`) plus
  `Gate`s (`config_converged`, `leader_elected`, `replica_caught_up{MaxLagEntries}`) and optional
  `PreActions` (`leadership_transfer`). Sequences never embed configs — each step's target is re-derived
  by folding mutations onto the base (`BuildStepConfig`); all generated ids are frozen at plan time
  (`deterministicReplicaId = sha256(baseHash|shardId|toNodeId)`), so resume is reproducible.
- `PlanAddNode` = 1 step. `PlanMoveShard` = 3 steps: add replica on target (gates: leader elected +
  caught up, MaxLag=0) → `bake` (soak `WaitFor`, config unchanged) → remove old replica (pre-action:
  transfer leadership off it). RF is preserved via a transient extra voter. `PlanSplitShard` errors
  (unimplemented; `StepSendCommand`/`ControlCommand` are placeholders for it).
- `Executor.Run(ctx, seq)`: verify base; per step — drift check (every node at current or target
  version, live `ValidateTransition`), run pre-actions, push target config to every node via
  `AdminPlane.UpdateClusterConfig` (falls back to `Bootstrap` on "not provisioned" — how add-node
  provisions the new node), `awaitConverged` (**≤2 config versions live at once**; full convergence
  before advancing), await gates, checkpoint `SaveSequence` (atomic). Resume = reload base+sequence,
  re-fold to cursor, continue. The executor never calls AddVoter/RemoveServer itself — membership
  deltas are applied node-side by the leader's reconciler.
- Gates poll `AdminPlane.ListReplicaStates`/`GetClusterConfig`; caught-up = follower
  `AppliedIndex+maxLag >= leader CommitIndex` (from `RaftStats`).

## Core invariants

- Keyspace per app = 4 bytes `[0x00000000, 0xffffffff]`, split into contiguous shards
  (inclusive bounds, no gaps/overlap; `Validate` enforces full coverage). Shard keys should be 4 bytes.
- A node hosts **≤1 replica per shard** (`Validate`: a shard's replicas are on distinct nodes).
  So `(node, shardId)` → unique replica; and future shard-split keeps `shardKey → active shard` unique.
- `Node.replicas` + `Node.clusterConfig` are a **matched pair** under `Node.mu` (RWMutex); they change
  together only in Start/Bootstrap/UpdateClusterConfig (via `reconcileReplicasLocked`). Read/Update
  snapshot both under one RLock, then release before slow work.
- Cores must be **deterministic, side-effect-free, explode on internal errors**. No `time.Now()`/rand
  inside apply; the request timestamp is leader-stamped in `Request.Now` (Unix nanos). Watch map iteration order.
- Raft log store **must be durable**: `WithSyncWrites(true)`. `node.go` sets it for the raft store;
  `store.DefaultOptions` defaults to `false` (footgun for any new raft-store caller).
- Config `Version` is the cluster-wide monotonic clock: providers adopt highest, transitions must
  increase it, executor convergence and drift checks compare it.

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
  `api.go` (core+stub interfaces, per-method type aliases), `adapters.go` (dispatch by `method_number`;
  declares per-core RPC Prometheus metrics + a `RegisterMetrics(prometheus.Registerer)`), `stubs.go`
  (`<Stub>MonsteraStub` + `<Stub>NonclusteredStub`).
- Per method `Foo`: needs `FooRequest`/`FooResponse` proto in `core_types_package`, each implementing
  `encoding.BinaryMarshaler`/`Unmarshaler`; sharded `*Request` implements `ShardKey() []byte`.
  These (MarshalBinary + ShardKey) are **user-provided**, NOT generated by monstera. Method numbers are
  wire-stable; never renumber.
- Core method signature: `Foo(*FooRequest) (*FooResponse, error)` where the aliases wrap payloads in
  `rpc.ReadRequest[*corepb.FooRequest]` etc. Domain errors go in `Response.ApplicationError` (`rpc.Error`),
  not as Go errors; `nilifyIfEmpty` treats OK/INVALID codes as nil.

## Metrics

- Framework metrics (`monstera_replica_*`, `monstera_raft_*`) live in `metrics.go` (node/replica layer,
  emitted at the `appCoreAdapter` boundary) and `internal/raft/metrics.go` (Raft layer). **All carry a
  `node` label** (node id) plus `{application, shard, replica}` (+ `result`/`reason`/`op` where
  relevant); snapshot metrics funnel through `raft.RecordSnapshot` (ops: persist/restore/install).
  Node-level gauges: `monstera_node_ready` (1 iff READY) and `monstera_config_version_number` (applied
  config version — compare across nodes for rollout progress), labeled `{node}` only.
- Created but **not auto-registered** (plain `prometheus.New*`, not `promauto`), so the caller controls
  the registry. Call `monstera.RegisterMetrics(registerer)` once at startup (pulls in Raft-layer vecs
  via `raft.Collectors()`). Generated code adds its own `RegisterMetrics(registerer)` for the per-core
  RPC metrics — call both.
- Register only in processes that actually host a node/cores (they emit nothing on pure clients like the
  worker/gateway or the nonclustered single-node stub, which calls raw cores, not the instrumented adapters).

## Concurrency map

- `Node.mu` RWMutex → replicas + clusterConfig. `Node.smu` Mutex → nodeState. Reconciler goroutine
  lifecycle via `reconcilerCancel`/`reconcilerDone`.
- `appCoreAdapter.coreMu` RWMutex → core Read vs Restore.
- `HraftBadgerStore.mu` → first/last index cache (committed only after `BatchUpdate` success).
- `Client.mu` → replicaStates + clusterConfig (refreshed by provider watch + `ListReplicaStates` loop).
- `DataPlaneClient`: `configMu` → clusterConfig; `streamsMu` → per-node raft streams;
  `raftMessageStream.pendingMu` → messageId→chan correlation map. `GrpcClientPool` mutex → conns.

## Open issues / not-yet-implemented

- Events / Pub-Sub bus (`core.Event`, `UpdateResponse.Events`) is API-only, unimplemented.
- Updates are at-least-once (forward/retry can double-apply) — rely on core idempotency.
- Shard split is planned, not implemented (`PlanSplitShard` errors; `send_command` step kind is a
  placeholder); key-based resolution already anticipates it.
- gRPC conns are insecure, no dial options/TLS yet. Raft bidi stream is one per node pair
  (potential head-of-line blocking across replicas).
- `notes/` design docs drift: `transport-planes-design.md` claims the proto is unchanged (it gained
  `Bootstrap`/`GetClusterConfig` and a RaftState enum-order fix); `sequences-design.md` predates the
  `bake` step and the `cluster` (not `control`) CLI naming. Proto `go_package` still points at an old
  `internal/monsterapb` path.

## Build & test

`make build` · `go test -race ./...` · `make lint`.
Fast inner loop: `go test -race ./rpc/... ./cluster/... ./store/... ./internal/raft/... ./control/... ./utils/...`
Integration tests (`internal/integration_test/`): shared fixtures in `testcore/` (PlaygroundCore in-mem KV,
NopCore, stub, descriptors — importable non-`_test` package); `node_stop/` (3-node gRPC cluster, kill a
node mid-traffic → failover, ~15s); `admin/` (bootstrap over the gRPC admin plane); `control/`
(add-node & move-shard sequences end-to-end over gRPC); `nodelifecycle/` (bootstrap idempotency, data-dir
layout, restart persistence, replica add/remove reconcile, replica-state stats).
