# Logging

`ApplicationCore.Read` and `Update` both take a `*slog.Logger`:

```go
Read(req []byte, log *slog.Logger) (*ReadResponse, error)
Update(req []byte, log *slog.Logger) (*UpdateResponse, error)
```

It looks like an ordinary logger, but the raft-replicated context `Update` runs in — and, to a lesser degree, the
concurrent, possibly-stale context `Read` runs in — gives it properties an ordinary logger doesn't have, worth
understanding before using it.

## Why it isn't a normal logger

`Update` runs from `appCoreAdapter.Apply`, which is hashicorp/raft's FSM `Apply` (see [Core
Principles](/docs/core-principles.md), Principle 1: determinism). That means a single client-visible write can cause
`log.Warn(...)` to execute:

* **once per replica** — a 3- or 5-node shard runs `Update` (and whatever it logs) on every replica, not just the
  leader;
* **again on replay** — a node restart replays its raft log tail since the last snapshot, and a rejoining or
  newly-added follower replays its entire catch-up range, both by calling `Apply` again for entries it (or a fresh
  replica) never saw live.

Neither is a bug. A line logged from inside `Update` is not "this happened once," it's "this replica applied this
log entry, live or replayed" — a different, and genuinely useful, signal for debugging core logic (comparing what
every replica computed for the same entry, or watching what a catching-up node does) precisely because it isn't
deduplicated for you. But it means this log must never be confused with, or mixed into, an ordinary request log —
see "A separate stream" below.

`Read` shares the first property (it may run on any replica, including a stale follower serving
`AllowReadFromFollowers`) but not the second: `Read` is never part of raft log replay, so a line logged from inside
it never carries a "replay" tag — that dimension is simply moot, not filtered out.

## `log` buffers; it doesn't write

`log` is backed by a handler that only buffers records — nothing is written when you call `log.Warn(...)`. The
framework reads the buffer back after `Read`/`Update` returns, however it returns:

* **success** — the buffered lines go through `CoreLogPolicy` (below).
* **a returned error** — everything buffered so far, plus a line the framework adds itself describing the error, is
  kept and written unfiltered (see "The fatal path" below).
* **a panic** — recovered, turned into an error, and the same fatal-path handling applies. You get this for free;
  there's nothing to set up in your core to make a panic still leave a log line behind.

That third case is exactly why `log` is passed in rather than collected from a return value: whatever you logged
before hitting the bug survives the panic, because the framework — not `Read`/`Update` — owns the buffer and reads
it back regardless of how the call exited.

Call it like any other `*slog.Logger` — `log.Debug`/`Info`/`Warn`/`Error`, with `key, value, ...` pairs:

```go
func (c *Core) AcquireLock(req *coreapis.AcquireLockRequest, log *slog.Logger) (*coreapis.AcquireLockResponse, error) {
    ...
    if !acquired {
        log.Warn("lock contention", "lock", req.Payload.LockId.LockName, "blocked_by", blockingLeaseId)
    }
    ...
}
```

## Disposition: `CoreLogPolicy`

What happens to a successful call's buffered lines is controlled by `NodeConfig.CoreLogPolicy`:

```go
type CoreLogPolicy struct {
    MinLevel      slog.Level // drop lines below this level
    LeaderOnly    bool       // keep only lines from the replica currently leading
    IncludeReplay bool       // keep lines produced during raft replay/catch-up, not just live commits
}
```

`DefaultCoreLogPolicy` (`LeaderOnly: true, IncludeReplay: false`) is the quiet-by-default setting: one line per
logical operation, only for genuinely new commits — close to what an ordinary logger would give you, despite living
inside the state machine. Loosen either field for a specific debugging session — `LeaderOnly: false` to compare
what every replica actually computed for a suspected divergence bug, `IncludeReplay: true` to watch what a specific
node does while catching up. Set `MinLevel` above `slog.LevelError` to disable the success path entirely for a core.

The same policy applies to `Read`'s lines, with `IncludeReplay` simply never mattering (nothing from `Read` is ever
tagged replay). `LeaderOnly` still does: it's a legitimate way to cut volume to one replica's worth of lines even
though a follower serving a read is itself perfectly valid.

## The fatal path always writes, unfiltered

A call that returns an error or panics ignores `CoreLogPolicy` completely and writes synchronously, before `Apply`
panics and the node goes down. Two reasons this path is special, not just louder:

* The dimensions `CoreLogPolicy` would otherwise filter on — which replica, live vs. replay — are exactly what you
  need unfiltered when a node is about to crash, not noise to cut.
* The success path is asynchronous specifically to never block a raft commit on log I/O. That protection stops
  mattering the moment the node is about to crash anyway — durability wins over not-blocking once there's no more
  commit throughput left to protect, so the fatal path blocks briefly and writes directly instead of queuing.

## A separate stream

Wire a destination with `NodeConfig.CoreLogDestination` — a plain `slog.Handler` (JSON to a file, text to stdout
tagged with a static label, whatever your deployment wants). Leave it `nil` and the mechanism stays fully active
(`log.Warn(...)` calls remain safe no-ops) but nothing is written anywhere.

Whatever you configure, keep it a destination of its own — never the same stream as a request/response log. Every
line carries `node_id`, `application_name`, `shard_id`, `replica_id`, `is_leader`, and `fatal`; lines from `Update`
additionally carry `replay` and the raft log `index` they applied at (both omitted from `Read`'s lines — replay
never applies, and there's no raft index to attach). The tagging is the point: multi-replica duplication is useful
here specifically because it's labeled well enough to tell replicas apart, and mixing any of this into a normal
request log would make the normal log unreadable and this one impossible to filter independently.

```go
nodeConfig := monstera.DefaultMonsteraNodeConfig
nodeConfig.CoreLogDestination = slog.NewJSONHandler(coreLogFile, nil)
nodeConfig.CoreLogPolicy = monstera.CoreLogPolicy{MinLevel: slog.LevelWarn, LeaderOnly: true, IncludeReplay: false}
```
