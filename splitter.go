package monstera

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"sort"
	"time"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/internal/replication"
	"github.com/evrblk/monstera/internal/replication/replicationpb"
)

// splitter is the node-local shard-split seeding pipeline for ONE parent
// replica (see notes/shard-split-design.md, Phase 2). It runs while the
// node's applied config says the parent shard is SPLITTING and this node
// hosts a parent replica, and it fills the durable state of the co-located
// DORMANT children:
//
//   - CoreTypeInMemory: copies the parent's snapshot as each child's base
//     (content verbatim, metadata rewritten) and tails the parent's applied
//     log, copying each committed entry into the log of the child that owns
//     its stamped shard key (a NOOP filler goes to the siblings, keeping
//     child index == parent index).
//   - CoreTypePersistedExclusive: restores the parent's snapshot into a live
//     child core (bounds-filtered by the core's portable Restore) and tails
//     the parent's applied log, applying each owned entry to the child core
//     through plain Update; progress is tracked durably as catchUpIndex.
//   - CoreTypePersistedShared: no splitter runs at all — the children's rows
//     are the parent's live rows.
//
// Everything the splitter writes is durable and resumable; it is stopped and
// restarted wholesale on every config apply and simply continues from its
// recorded progress. It is self-healing: whenever it cannot continue (parent
// log compacted past the seed, an unstamped update entry from a leader that
// predates the splitting config), it restarts the affected child's seed from
// a fresh parent snapshot at a later base index.
type splitter struct {
	parent   *replica
	coreType CoreType
	children []*splitChild
	factory  func(*cluster.Shard, *cluster.Replica) ApplicationCore

	// promote is the node callback that promotes the seeded dormant children
	// into serving replicas; called once, after cutoff finalization.
	promote func() error

	codec  replication.CommandCodec
	logger *log.Logger

	cancel context.CancelFunc
	done   chan struct{}
}

// splitChild is one co-located dormant child the splitter seeds.
type splitChild struct {
	shard      *cluster.Shard // bounds (immutable during a split)
	replicaSet []raft.RaftServer
	dormant    *dormantReplica

	// core is the live child core for CoreTypePersistedExclusive (owned by
	// the splitter, closed on stop); nil for CoreTypeInMemory.
	core ApplicationCore

	// seeded is the in-memory copy of the durable seed progress: the last
	// parent log index reflected in this child's seed.
	seeded uint64
	// hasBase reports whether the child has a base (a seeded base snapshot
	// for CoreTypeInMemory; a restored core with recorded catchUpIndex for
	// CoreTypePersistedExclusive).
	hasBase bool
}

// splitterPollInterval is how often the splitter re-checks the parent's
// applied index when it is fully caught up.
const splitterPollInterval = 50 * time.Millisecond

// splitterBatchSize bounds how many parent entries are processed per child
// batch (one AppendEntries call / one catchUpIndex write).
const splitterBatchSize = 256

func newSplitter(parent *replica, coreType CoreType, children []*splitChild,
	factory func(*cluster.Shard, *cluster.Replica) ApplicationCore, promote func() error, logger *log.Logger) *splitter {
	// Sort children by lower bound so routing can mirror FindShardByShardKey.
	sort.Slice(children, func(i, j int) bool {
		return bytes.Compare(children[i].shard.LowerBound, children[j].shard.LowerBound) < 0
	})
	return &splitter{
		parent:   parent,
		coreType: coreType,
		children: children,
		factory:  factory,
		promote:  promote,
		codec:    &replication.ProtoCommandCodec{},
		logger:   logger,
	}
}

// start launches the splitter goroutine.
func (s *splitter) start() {
	ctx, cancel := context.WithCancel(context.Background())
	s.cancel = cancel
	s.done = make(chan struct{})

	go func() {
		defer close(s.done)
		s.run(ctx)
	}()
}

// stop terminates the splitter and waits for it to exit. Idempotent.
func (s *splitter) stop() {
	if s.cancel == nil {
		return
	}
	s.cancel()
	<-s.done
	s.cancel = nil

	for _, ch := range s.children {
		if ch.core != nil {
			ch.core.Close()
			ch.core = nil
		}
	}
}

func (s *splitter) run(ctx context.Context) {
	ticker := time.NewTicker(splitterPollInterval)
	defer ticker.Stop()

	for {
		if err := s.step(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Printf("split seeding of shard %s: %v (will retry)", s.parent.shardId, err)
		} else if m := s.parent.frozenAt(); m > 0 {
			// The CUTOFF applied at m: drain the seed to exactly m, finalize
			// the children's Raft state, and promote them in place.
			done, err := s.finalize(m)
			if err != nil {
				s.logger.Printf("split finalization of shard %s: %v (will retry)", s.parent.shardId, err)
			} else if done {
				s.logger.Printf("Split of shard %s finalized at cutoff index %d; children promoted", s.parent.shardId, m)
				return
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// finalize completes the split once the parent froze at m: it verifies every
// child's seed reaches exactly m, writes the persisted children's base
// snapshots (metadata-only at m, empty log — they replay nothing), primes the
// children's stable stores, and promotes them into serving replicas. Returns
// false (retry later) while children are still draining to m.
func (s *splitter) finalize(m uint64) (bool, error) {
	if s.coreType != CoreTypePersistedShared {
		for _, ch := range s.children {
			if ch.seeded < m {
				return false, nil // still draining; step() keeps copying
			}
		}
	}

	for _, ch := range s.children {
		switch s.coreType {
		case CoreTypePersistedShared, CoreTypePersistedExclusive:
			// Persisted children start with lastApplied = m and replay
			// nothing: metadata-only base snapshot at m, empty log.
			base, err := ch.dormant.seeder.LatestBaseIndex()
			if err != nil {
				return false, err
			}
			if base < m {
				if err := ch.dormant.seeder.SeedBaseSnapshot(m, ch.replicaSet, nil); err != nil {
					return false, err
				}
			}
		}
		if err := ch.dormant.seeder.Finalize(); err != nil {
			return false, err
		}
		if ch.core != nil {
			// The splitter-owned seeding core hands over to the replica's own.
			ch.core.Close()
			ch.core = nil
		}
	}

	if err := s.promote(); err != nil {
		return false, err
	}
	return true, nil
}

// step makes seeding progress: establishes missing bases and copies/applies
// the parent log tail up to the parent's current applied index. It returns
// early on any error; the run loop retries.
func (s *splitter) step(ctx context.Context) error {
	// Fully shared stores need no seeding at all: the children's rows are the
	// parent's live rows. The splitter exists only to finalize at the cutoff.
	if s.coreType == CoreTypePersistedShared {
		return nil
	}

	// Recover durable progress on the first pass (and after base restarts).
	for _, ch := range s.children {
		if ch.hasBase {
			continue
		}
		if err := s.recoverOrSeedBase(ch); err != nil {
			return fmt.Errorf("base seed of child %s: %w", ch.shard.Id, err)
		}
	}

	applied := s.parent.GetRaftStats().AppliedIndex
	// Once the parent froze at m, the seed ends at exactly m: entries after it
	// are deterministic rejections that mutated nothing, and the children's
	// own Raft groups take over from m+1.
	if m := s.parent.frozenAt(); m > 0 && m < applied {
		applied = m
	}
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lowest := s.lowestSeeded()
		if lowest >= applied {
			return nil // fully caught up
		}

		to := min(lowest+splitterBatchSize, applied)
		if err := s.copyRange(lowest+1, to); err != nil {
			return err
		}
	}
}

// lowestSeeded returns the least seed progress across children — the next
// copyRange starts right after it. Children ahead of it skip already-seeded
// entries idempotently.
func (s *splitter) lowestSeeded() uint64 {
	lowest := s.children[0].seeded
	for _, ch := range s.children[1:] {
		if ch.seeded < lowest {
			lowest = ch.seeded
		}
	}
	return lowest
}

// recoverOrSeedBase brings a child to the "has a base" state: it recovers
// durable progress recorded by a previous splitter run, or seeds a fresh base
// from the parent's snapshot.
func (s *splitter) recoverOrSeedBase(ch *splitChild) error {
	switch s.coreType {
	case CoreTypeInMemory:
		base, err := ch.dormant.seeder.LatestBaseIndex()
		if err != nil {
			return err
		}
		if base == 0 {
			base, err = s.seedBaseSnapshot(ch)
			if err != nil {
				return err
			}
		}
		last, err := ch.dormant.seeder.LastSeededIndex()
		if err != nil {
			return err
		}
		ch.seeded = max(base, last)
		ch.hasBase = true
		return nil

	case CoreTypePersistedExclusive:
		if ch.core == nil {
			// The child core is constructed once per splitter lifetime; the
			// factory receives the child shard so the core knows its bounds.
			ch.core = s.factory(ch.shard, replicaOf(ch.shard, ch.dormant.replicaId))
		}
		idx, err := ch.dormant.seeder.CatchUpIndex()
		if err != nil {
			return err
		}
		if idx == 0 {
			meta, rc, err := s.parent.TakeAndOpenSnapshot()
			if err != nil {
				return err
			}
			// The core's portable Restore keeps only the child's half.
			if err := ch.core.Restore(rc); err != nil {
				return fmt.Errorf("restoring parent snapshot into child core: %w", err)
			}
			if err := ch.dormant.seeder.SetCatchUpIndex(meta.Index); err != nil {
				return err
			}
			idx = meta.Index
		}
		ch.seeded = idx
		ch.hasBase = true
		return nil

	default:
		return fmt.Errorf("unexpected core type %v in splitter", s.coreType)
	}
}

// seedBaseSnapshot copies the parent's snapshot into the child's snapshot
// store as its base (content verbatim, metadata rewritten to the child's
// identity and membership) and returns the base index.
func (s *splitter) seedBaseSnapshot(ch *splitChild) (uint64, error) {
	meta, rc, err := s.parent.TakeAndOpenSnapshot()
	if err != nil {
		return 0, err
	}
	defer rc.Close()

	if err := ch.dormant.seeder.SeedBaseSnapshot(meta.Index, ch.replicaSet, rc); err != nil {
		return 0, err
	}
	s.logger.Printf("Seeded base snapshot for child %s at parent index %d", ch.shard.Id, meta.Index)
	return meta.Index, nil
}

// restartChild discards a child's seed progress and forces a fresh base at a
// later parent index (the self-healing path).
func (s *splitter) restartChild(ch *splitChild, reason string) error {
	s.logger.Printf("Restarting seed of child %s from a fresh base: %s", ch.shard.Id, reason)

	switch s.coreType {
	case CoreTypeInMemory:
		if err := ch.dormant.seeder.ResetLog(); err != nil {
			return err
		}
		base, err := s.seedBaseSnapshot(ch)
		if err != nil {
			return err
		}
		ch.seeded = base
		return nil

	case CoreTypePersistedExclusive:
		meta, rc, err := s.parent.TakeAndOpenSnapshot()
		if err != nil {
			return err
		}
		// Restore replaces the core state wholesale.
		if err := ch.core.Restore(rc); err != nil {
			return err
		}
		if err := ch.dormant.seeder.SetCatchUpIndex(meta.Index); err != nil {
			return err
		}
		ch.seeded = meta.Index
		return nil

	default:
		return fmt.Errorf("unexpected core type %v in splitter", s.coreType)
	}
}

// copyRange processes parent log entries (from, to] into every child that has
// not seeded them yet.
func (s *splitter) copyRange(from, to uint64) error {
	type routed struct {
		index uint64
		// owner is the child that receives the full entry; nil means every
		// child receives it in full (unsharded updates, non-update entries
		// copied as NOOPs have data == nil instead).
		owner *splitChild
		data  []byte // full MonsteraCommand bytes; nil => NOOP filler for everyone
	}

	entries := make([]routed, 0, to-from+1)
	for i := from; i <= to; i++ {
		e, err := s.parent.GetLogEntry(i)
		if err != nil {
			if errors.Is(err, raft.ErrLogEntryNotFound) {
				// Compacted past the seed: restart lagging children from a
				// fresh base.
				return s.restartLagging(i, "parent log compacted past the seed")
			}
			return err
		}

		if !e.IsCommand {
			// Raft-internal entry (membership, barrier): NOOP filler.
			entries = append(entries, routed{index: i})
			continue
		}

		cmd, err := s.codec.Decode(e.Data)
		if err != nil {
			return fmt.Errorf("decoding parent log entry %d: %w", i, err)
		}

		switch cmd.Type {
		case replicationpb.CommandType_COMMAND_TYPE_UPDATE:
			if !cmd.Stamped {
				// Proposed by a leader that had not applied the splitting
				// config yet: unroutable. Restart from a base past it.
				return s.restartLagging(i, "unstamped update entry in the seed tail")
			}
			if len(cmd.ShardKey) == 0 {
				// Unsharded update: every child receives it in full.
				entries = append(entries, routed{index: i, data: e.Data})
				continue
			}
			owner := s.childOwning(cmd.ShardKey)
			if owner == nil {
				// The children partition the parent's range and the key was
				// routed to the parent: this cannot happen on a valid config.
				panic(fmt.Sprintf("split seeding of shard %s: no child owns shard key %x", s.parent.shardId, cmd.ShardKey))
			}
			entries = append(entries, routed{index: i, owner: owner, data: e.Data})

		default:
			// Framework commands (NOOP, CUTOFF) carry no application state
			// during seeding; children get a NOOP at this index. (The cutoff
			// itself is handled by cutoff finalization, not the tailer.)
			entries = append(entries, routed{index: i})
		}
	}

	noop, err := s.codec.Encode(&replicationpb.MonsteraCommand{Type: replicationpb.CommandType_COMMAND_TYPE_NOOP})
	if err != nil {
		return err
	}

	for _, ch := range s.children {
		batch := make([]raft.SeedEntry, 0, len(entries))
		for _, e := range entries {
			if e.index <= ch.seeded {
				continue // already seeded by a previous run
			}
			switch s.coreType {
			case CoreTypeInMemory:
				data := noop
				if e.data != nil && (e.owner == nil || e.owner == ch) {
					data = e.data
				}
				batch = append(batch, raft.SeedEntry{Index: e.index, Data: data})

			case CoreTypePersistedExclusive:
				if e.data != nil && (e.owner == nil || e.owner == ch) {
					cmd, err := s.codec.Decode(e.data)
					if err != nil {
						return err
					}
					// A first apply into the child's private store. Core
					// errors explode, mirroring the FSM apply contract.
					if _, err := ch.core.Update(cmd.Payload); err != nil {
						panic(fmt.Sprintf("split catch-up of child %s: core.Update failed at parent index %d: %v", ch.shard.Id, e.index, err))
					}
				}
			}
		}

		switch s.coreType {
		case CoreTypeInMemory:
			if err := ch.dormant.seeder.AppendEntries(batch); err != nil {
				return err
			}
		case CoreTypePersistedExclusive:
			if err := ch.dormant.seeder.SetCatchUpIndex(to); err != nil {
				return err
			}
		}
		ch.seeded = to
	}

	return nil
}

// restartLagging restarts every child whose seed does not include index yet.
func (s *splitter) restartLagging(index uint64, reason string) error {
	for _, ch := range s.children {
		if ch.seeded >= index {
			continue
		}
		if err := s.restartChild(ch, reason); err != nil {
			return err
		}
	}
	return nil
}

// childOwning routes a stamped shard key to the child whose bounds contain
// it, mirroring cluster.FindShardByShardKey (children are sorted by lower
// bound; keys shorter than 4 bytes are prefixes).
func (s *splitter) childOwning(shardKey []byte) *splitChild {
	i := sort.Search(len(s.children), func(i int) bool {
		return bytes.Compare(s.children[i].shard.LowerBound, shardKey) > 0
	})
	if i == 0 {
		return nil
	}
	candidate := s.children[i-1]
	if bytes.Compare(shardKey, candidate.shard.UpperBound) <= 0 {
		return candidate
	}
	return nil
}

// shardOwningKey returns the shard from shards whose bounds contain shardKey,
// or nil. Same routing rules as cluster.FindShardByShardKey.
func shardOwningKey(shards []*cluster.Shard, shardKey []byte) *cluster.Shard {
	sorted := make([]*cluster.Shard, len(shards))
	copy(sorted, shards)
	sort.Slice(sorted, func(i, j int) bool {
		return bytes.Compare(sorted[i].LowerBound, sorted[j].LowerBound) < 0
	})
	i := sort.Search(len(sorted), func(i int) bool {
		return bytes.Compare(sorted[i].LowerBound, shardKey) > 0
	})
	if i == 0 {
		return nil
	}
	candidate := sorted[i-1]
	if bytes.Compare(shardKey, candidate.UpperBound) <= 0 {
		return candidate
	}
	return nil
}

// replicaOf finds the replica entry with the given id in a shard.
func replicaOf(shard *cluster.Shard, replicaId string) *cluster.Replica {
	for _, r := range shard.Replicas {
		if r.Id == replicaId {
			return r
		}
	}
	return nil
}
