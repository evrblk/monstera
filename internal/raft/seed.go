package raft

import (
	"errors"
	"fmt"
	"io"

	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/store"
)

// Shard-split seeding writes a DORMANT replica's durable Raft state — its
// base snapshot, log entries, and stable-store term — without constructing a
// live hraft.Raft. When seeding completes (at the split cutoff), the replica
// is promoted in place by constructing a regular Raft over the same stores:
// hashicorp/raft reads the pre-baked state exactly as if an established
// member had restarted (membership comes from the base snapshot metadata, so
// no BootstrapCluster call is ever made). See notes/shard-split-design.md.

// seedTerm is the Raft term of everything a Seeder writes: the base snapshot
// metadata, every copied log entry, and the primed CurrentTerm. Different
// nodes seed the same child from different base indexes, so terms carried
// over from the parent's log would disagree between replicas; a constant
// term makes all replicas of a seeded group consistent by construction. The
// child's first election then starts at seedTerm+1.
const seedTerm = 1

// keyCurrentTerm is the stable-store key hashicorp/raft persists its current
// term under. Not exported by the library; the value is stable (see
// hashicorp/raft constants) and covered by TestSeederClonesRaftGroup.
var keyCurrentTerm = []byte("CurrentTerm")

var (
	// ErrSeedGap means AppendEntries was called with entries that are not
	// contiguous (internally, or with the already-seeded log). A gap would
	// panic hashicorp/raft at startup, which scans every entry between the
	// base snapshot and the last log index.
	ErrSeedGap = errors.New("seed entries are not contiguous")
)

// SeedEntry is one log entry to copy into a dormant replica's log. Data is
// the replicated command bytes (for shard splits: a MonsteraCommand — either
// a routed application update or a framework NOOP filler). All seeded entries
// are written as Raft command entries at seedTerm.
type SeedEntry struct {
	Index uint64
	Data  []byte
}

// Seeder writes the durable Raft state of one dormant replica. It must not
// coexist with a live Raft for the same replica id: discard the Seeder before
// constructing the Raft that promotes the replica (both cache log index
// bounds, and only the writer's cache is maintained).
//
// Everything a Seeder writes is durable and resumable: after a crash,
// LastSeededIndex/CatchUpIndex tell the caller where to resume, and repeating
// a write (same base snapshot, same entries) is idempotent.
type Seeder struct {
	replicaId string
	hstore    *HraftBadgerStore
	hfss      *hraft.FileSnapshotStore
}

// NewSeeder opens the seeding-side view of a replica's durable Raft state:
// the log/stable store under the replica-id prefix of the shared raft Badger,
// and the file snapshot store under <baseDir>/snapshots/<replicaId> — the
// exact stores a live Raft for this replica would use.
func NewSeeder(baseDir string, replicaId string, raftStore *store.BadgerStore) *Seeder {
	return &Seeder{
		replicaId: replicaId,
		hstore:    NewHraftBadgerStore(raftStore, []byte(replicaId), &protoLogCodec{}),
		hfss:      newFileSnapshotStore(baseDir, replicaId),
	}
}

// SeedBaseSnapshot writes the replica's base snapshot with rewritten
// metadata: Index=index, Term=seedTerm, and the group membership carried in
// the snapshot metadata (which is how the promoted replica learns its
// configuration without BootstrapCluster).
//
// content is the snapshot payload, typically the parent's snapshot stream
// copied verbatim (valid for the child because core Restore is portable and
// bounds-filtered). A nil content writes a metadata-only snapshot — the
// persisted-core construction where the child's store itself is the state
// and there is nothing to restore (Index=M, empty log).
//
// Calling SeedBaseSnapshot again (e.g. a base refresh at a later index, or a
// crash-retry) writes a newer snapshot; hashicorp/raft always starts from the
// latest one.
func (s *Seeder) SeedBaseSnapshot(index uint64, servers []RaftServer, content io.Reader) error {
	if index == 0 {
		return fmt.Errorf("base snapshot index must be greater than 0")
	}
	if len(servers) == 0 {
		return fmt.Errorf("base snapshot must carry the group membership")
	}

	hservers := make([]hraft.Server, len(servers))
	for i, srv := range servers {
		hservers[i] = hraft.Server{
			Suffrage: hraft.Voter,
			ID:       hraft.ServerID(srv.ReplicaId),
			Address:  hraft.ServerAddress(srv.NodeId),
		}
	}
	configuration := hraft.Configuration{Servers: hservers}

	// Version 1 snapshots carry the configuration in the metadata directly;
	// the transport argument is only used to encode the legacy peers field,
	// which is never read back for version 1.
	sink, err := s.hfss.Create(hraft.SnapshotVersion(1), index, seedTerm, configuration, index, seedPeerEncoder{})
	if err != nil {
		return fmt.Errorf("snapshot store Create: %w", err)
	}

	if content != nil {
		if _, err := io.Copy(sink, content); err != nil {
			sink.Cancel()
			return fmt.Errorf("copying base snapshot content: %w", err)
		}
	}

	return sink.Close()
}

// AppendEntries appends copied log entries. Entries must be contiguous,
// internally and with the already-seeded log (the first entry after
// SeedBaseSnapshot(index) must be index+1; the caller owns that alignment
// since the log store does not know the base). Re-appending already-seeded
// indexes is idempotent (same content by construction — the parent log is
// the single source).
func (s *Seeder) AppendEntries(entries []SeedEntry) error {
	if len(entries) == 0 {
		return nil
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("%w: %d follows %d", ErrSeedGap, entries[i].Index, entries[i-1].Index)
		}
	}
	last, err := s.hstore.LastIndex()
	if err != nil {
		return err
	}
	if last > 0 && entries[0].Index > last+1 {
		return fmt.Errorf("%w: first entry %d, seeded log ends at %d", ErrSeedGap, entries[0].Index, last)
	}

	logs := make([]*hraft.Log, len(entries))
	for i, e := range entries {
		logs[i] = &hraft.Log{
			Index: e.Index,
			Term:  seedTerm,
			Type:  hraft.LogCommand,
			Data:  e.Data,
		}
	}
	return s.hstore.StoreLogs(logs)
}

// LastSeededIndex returns the last log index written (0 if none). For
// log-seeded (in-memory) children this is the seed progress; a restarted
// splitter resumes from the next index.
func (s *Seeder) LastSeededIndex() (uint64, error) {
	return s.hstore.LastIndex()
}

// LatestBaseIndex returns the index of the latest seeded base snapshot, or 0
// if none exists yet. Used by a restarted splitter to discover its resume
// point.
func (s *Seeder) LatestBaseIndex() (uint64, error) {
	metas, err := s.hfss.List()
	if err != nil {
		return 0, err
	}
	if len(metas) == 0 {
		return 0, nil
	}
	// List returns snapshots sorted newest first.
	return metas[0].Index, nil
}

// ResetLog deletes all seeded log entries. Used when a seed must restart from
// a fresh base snapshot (e.g. the unstamped-tail guard, or the parent's log
// was compacted past the seed progress): a new base at a later index followed
// by appends from that index would otherwise leave a gap in the log store's
// index bookkeeping.
func (s *Seeder) ResetLog() error {
	first, err := s.hstore.FirstIndex()
	if err != nil {
		return err
	}
	last, err := s.hstore.LastIndex()
	if err != nil {
		return err
	}
	if last == 0 {
		return nil
	}
	return s.hstore.DeleteRange(first, last)
}

// keyCatchUpIndex is a monstera-owned stable-store key (distinct from the
// keys hashicorp/raft uses) recording catch-up progress for children that are
// seeded by applying entries to the core instead of copying the log
// (CoreTypePersistedExclusive).
var keyCatchUpIndex = []byte("MonsteraCatchUpIndex")

// keyCutoffIndex is a monstera-owned stable-store key recording that this
// (parent) replica's shard was frozen by a split CUTOFF committed at the
// stored log index. The marker — not log replay — is what makes the freeze
// survive restarts for every core type (a snapshot taken after the cutoff
// would truncate the CUTOFF entry out of the replayable log).
var keyCutoffIndex = []byte("MonsteraCutoffIndex")

// CutoffMarker reads and writes a replica's durable split-cutoff marker. It
// operates on the same stable-store keyspace a live Raft for the replica
// uses, so it may coexist with one (stable-store reads/writes do not touch
// the log index cache).
type CutoffMarker struct {
	hstore *HraftBadgerStore
}

// NewCutoffMarker opens the marker accessor for a replica.
func NewCutoffMarker(raftStore *store.BadgerStore, replicaId string) *CutoffMarker {
	return &CutoffMarker{
		hstore: NewHraftBadgerStore(raftStore, []byte(replicaId), &protoLogCodec{}),
	}
}

// Set durably records the cutoff index. Idempotent.
func (m *CutoffMarker) Set(index uint64) error {
	return m.hstore.SetUint64(keyCutoffIndex, index)
}

// Get returns the recorded cutoff index, 0 if the shard was never frozen.
func (m *CutoffMarker) Get() (uint64, error) {
	v, err := m.hstore.GetUint64(keyCutoffIndex)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// SetCatchUpIndex durably records that entries up to and including index have
// been applied to the child core.
func (s *Seeder) SetCatchUpIndex(index uint64) error {
	return s.hstore.SetUint64(keyCatchUpIndex, index)
}

// CatchUpIndex returns the recorded catch-up progress, 0 if none.
func (s *Seeder) CatchUpIndex() (uint64, error) {
	v, err := s.hstore.GetUint64(keyCatchUpIndex)
	if err != nil {
		if errors.Is(err, errNotFound) {
			return 0, nil
		}
		return 0, err
	}
	return v, nil
}

// Finalize primes the stable store so the promoted replica starts at
// seedTerm (its first election bumps to seedTerm+1, keeping "term seedTerm =
// seeded" unambiguous). Idempotent; call once seeding is complete, before
// constructing the live Raft.
func (s *Seeder) Finalize() error {
	return s.hstore.SetUint64(keyCurrentTerm, seedTerm)
}

// seedPeerEncoder satisfies the Transport parameter of SnapshotStore.Create.
// Version 1 snapshots only invoke EncodePeer (to fill the legacy peers field,
// never read back for version 1); every other method panics via the nil
// embedded interface.
type seedPeerEncoder struct{ hraft.Transport }

func (seedPeerEncoder) EncodePeer(id hraft.ServerID, addr hraft.ServerAddress) []byte {
	return []byte(addr)
}
