package monstera

import (
	"errors"
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
type Router struct {
	nodesById    map[string]*cluster.Node
	shardsById   map[string]*cluster.Shard
	replicasById map[string]*cluster.Replica
	apps         map[string]*routerApp
}

// routerApp holds an application's routing index: the routable shards (active or
// splitting), sorted by lower bound. Because the routable shards form a contiguous,
// non-overlapping partition of the keyspace, FindShardByShardKey can binary-search
// this slice directly, and inactive/activating shards are excluded from both routing
// and ListRoutableShards.
type routerApp struct {
	routable []routableShard
}

// routableShard is one entry of the routing index: a routable shard with its bounds.
type routableShard struct {
	lower cluster.ShardKey
	upper cluster.ShardKey
	shard *cluster.Shard
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
			routable: make([]routableShard, 0, len(a.Shards)),
		}
		for _, s := range a.Shards {
			r.shardsById[s.Id] = s
			for _, rep := range s.Replicas {
				r.replicasById[rep.Id] = rep
			}
			if s.IsRoutable() {
				app.routable = append(app.routable, routableShard{
					lower: s.LowerKey(),
					upper: s.UpperKey(),
					shard: s,
				})
			}
		}
		// Routable shards never share a lower bound (they partition the
		// keyspace), so sorting by lower alone is total.
		sort.Slice(app.routable, func(i, j int) bool {
			return app.routable[i].lower < app.routable[j].lower
		})
		r.apps[a.Name] = app
	}

	return r
}

// FindShardByShardKey returns the routable (active or splitting) shard whose
// [LowerKey, UpperKey] range contains shardKey. Inactive and activating shards
// may overlap that range and are never returned. Every ShardKey value is valid;
// on a validated config (routable shards cover the whole keyspace) the lookup
// only fails when the application is unknown.
func (r *Router) FindShardByShardKey(applicationName string, shardKey cluster.ShardKey) (*cluster.Shard, error) {
	app, ok := r.apps[applicationName]
	if !ok {
		return nil, errRouteApplicationNotFound
	}

	shards := app.routable

	// The routable shards partition the keyspace with no gaps or overlaps, so
	// the shard containing shardKey is the one with the greatest lower bound
	// that is <= shardKey. Find the first shard with a lower bound strictly
	// greater than shardKey; the one immediately before it is that candidate.
	i := sort.Search(len(shards), func(i int) bool {
		return shards[i].lower > shardKey
	})
	if i == 0 {
		return nil, errRouteShardNotFound
	}
	candidate := shards[i-1]
	if shardKey <= candidate.upper {
		return candidate.shard, nil
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
	for i, rs := range app.routable {
		out[i] = rs.shard
	}
	return out, nil
}
