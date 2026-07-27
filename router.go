package monstera

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/evrblk/monstera/cluster"
)

var (
	errRouteApplicationNotFound = errors.New("application not found")
	errRouteShardNotFound       = errors.New("shard not found")
	errRouteReplicaNotFound     = errors.New("replica not found")
	errRouteNodeNotFound        = errors.New("node not found")
)

// Router is an immutable, index-backed view of a cluster.Config used for the
// request serving path: it resolves a shard key to its owning shard and looks up
// shards, replicas and nodes by id in O(1)/O(log n) instead of scanning the
// config.
//
// A Router is built once from a config (see NewRouter) and never mutated, so —
// unlike the cluster.Config it is built from — it is safe for concurrent reads.
// Callers that swap in a new config (a node applying a cluster config update, a
// client adopting a polled config) build a fresh Router alongside it and publish
// the pair together.
//
// Crucially, routing correctness does NOT depend on the ordering of the source
// config's shard slices. NewRouter builds its own sorted index of the routable
// shards, so a config applied over RPC (which is not normalized by the config
// package's Load* functions) still routes correctly. This is why key routing
// lives here and not on cluster.Config.
type Router struct {
	nodesById    map[string]*cluster.Node
	shardsById   map[string]*cluster.Shard
	replicasById map[string]*cluster.Replica
	apps         map[string]*routerApp
}

// routerApp holds an application's routing index: the routable shards (active or
// splitting), sorted by lower bound. Because the routable shards form a
// contiguous, non-overlapping partition of the keyspace, FindShardByShardKey can
// binary-search this slice directly, and inactive/activating shards are excluded
// from both routing and ListRoutableShards.
type routerApp struct {
	routable []*cluster.Shard
}

// NewRouter builds a Router from cfg. cfg is only read, never retained beyond
// construction (the Router aliases cfg's shard/replica/node pointers, which are
// treated as read-only, but does not depend on cfg's slice ordering afterwards).
//
// A nil cfg yields an empty Router whose lookups all report "not found"; this
// lets a not-yet-provisioned node or a client without a config hold a non-nil
// Router without special-casing every call site.
func NewRouter(cfg *cluster.Config) *Router {
	r := &Router{
		nodesById:    make(map[string]*cluster.Node),
		shardsById:   make(map[string]*cluster.Shard),
		replicasById: make(map[string]*cluster.Replica),
		apps:         make(map[string]*routerApp),
	}
	if cfg == nil {
		return r
	}

	for _, n := range cfg.Nodes {
		r.nodesById[n.Id] = n
	}

	for _, a := range cfg.Applications {
		app := &routerApp{
			routable: make([]*cluster.Shard, 0, len(a.Shards)),
		}
		for _, s := range a.Shards {
			r.shardsById[s.Id] = s
			for _, rep := range s.Replicas {
				r.replicasById[rep.Id] = rep
			}
			if s.IsRoutable() {
				app.routable = append(app.routable, s)
			}
		}
		sortShardsByLowerBound(app.routable)
		r.apps[a.Name] = app
	}

	return r
}

// sortShardsByLowerBound sorts shards in place by lower bound, breaking ties by
// id (an inactive parent and its active child can share a lower bound after a
// split, though only one of them is ever routable).
func sortShardsByLowerBound(shards []*cluster.Shard) {
	sort.Slice(shards, func(i, j int) bool {
		if cmp := bytes.Compare(shards[i].LowerBound, shards[j].LowerBound); cmp != 0 {
			return cmp < 0
		}
		return shards[i].Id < shards[j].Id
	})
}

// FindShardByShardKey returns the routable (active or splitting) shard whose
// [LowerBound, UpperBound] range contains shardKey. Inactive and activating
// shards may overlap that range and are never returned.
//
// shardKey is compared byte-wise, so a key shorter than 4 bytes is treated as a
// prefix (conceptually padded with 0x00). A key longer than the 4-byte keyspace
// is rejected.
func (r *Router) FindShardByShardKey(applicationName string, shardKey []byte) (*cluster.Shard, error) {
	if len(shardKey) == 0 || len(shardKey) > 4 {
		return nil, fmt.Errorf("invalid shard key length %d: must be between 1 and 4 bytes", len(shardKey))
	}

	app, ok := r.apps[applicationName]
	if !ok {
		return nil, errRouteApplicationNotFound
	}

	shards := app.routable

	// The routable shards partition the keyspace with no gaps or overlaps, so
	// the shard containing shardKey is the one with the greatest LowerBound that
	// is <= shardKey. Find the first shard with LowerBound strictly greater than
	// shardKey; the one immediately before it is that candidate.
	i := sort.Search(len(shards), func(i int) bool {
		return bytes.Compare(shards[i].LowerBound, shardKey) > 0
	})
	if i == 0 {
		return nil, errRouteShardNotFound
	}
	candidate := shards[i-1]
	if bytes.Compare(shardKey, candidate.UpperBound) <= 0 {
		return candidate, nil
	}
	return nil, errRouteShardNotFound
}

// GetShard returns the shard with the given id, or errRouteShardNotFound. It
// finds shards in any state (routing state is irrelevant for a by-id lookup).
func (r *Router) GetShard(shardId string) (*cluster.Shard, error) {
	if s, ok := r.shardsById[shardId]; ok {
		return s, nil
	}
	return nil, errRouteShardNotFound
}

// GetReplica returns the replica with the given id, or errRouteReplicaNotFound.
func (r *Router) GetReplica(replicaId string) (*cluster.Replica, error) {
	if rep, ok := r.replicasById[replicaId]; ok {
		return rep, nil
	}
	return nil, errRouteReplicaNotFound
}

// GetNode returns the node with the given id, or errRouteNodeNotFound.
func (r *Router) GetNode(nodeId string) (*cluster.Node, error) {
	if n, ok := r.nodesById[nodeId]; ok {
		return n, nil
	}
	return nil, errRouteNodeNotFound
}

// ListRoutableShards returns the application's routable shards (active or splitting),
// sorted by lower bound. These are exactly the shards that currently serve the
// keyspace, which is what fanout operations (e.g. running GC on every shard)
// must target: inactive shards are retired and serve nothing, and activating
// shards are not serving yet (their range is still covered by the splitting
// parent). The returned slice is a copy the caller may retain; the shard
// pointers in it alias the config and are read-only.
func (r *Router) ListRoutableShards(applicationName string) ([]*cluster.Shard, error) {
	app, ok := r.apps[applicationName]
	if !ok {
		return nil, errRouteApplicationNotFound
	}
	out := make([]*cluster.Shard, len(app.routable))
	copy(out, app.routable)
	return out, nil
}
