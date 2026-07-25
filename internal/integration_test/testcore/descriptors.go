package testcore

import (
	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/store"
)

// InMemoryPlaygroundDescriptors registers the "Core" application backed by
// InMemoryPlaygroundCore (CoreTypeInMemory).
func InMemoryPlaygroundDescriptors() monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			CoreType: monstera.CoreTypeInMemory,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NewBoundedInMemoryPlaygroundCore(shard.LowerBound, shard.UpperBound)
			},
		},
	}
}

// SharedPlaygroundDescriptors registers the "Core" application backed by
// SharedPlaygroundCore (CoreTypePersistedShared) over the given node-wide
// store, which the caller owns.
func SharedPlaygroundDescriptors(s *store.BadgerStore) monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			CoreType: monstera.CoreTypePersistedShared,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NewSharedPlaygroundCore(s, shard.LowerBound, shard.UpperBound)
			},
		},
	}
}

// ExclusivePlaygroundDescriptors registers the "Core" application backed by
// ExclusivePlaygroundCore (CoreTypePersistedExclusive) over the given
// node-wide store, which the caller owns.
func ExclusivePlaygroundDescriptors(s *store.BadgerStore) monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			CoreType: monstera.CoreTypePersistedExclusive,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NewExclusivePlaygroundCore(s, shard.Id, shard.LowerBound, shard.UpperBound)
			},
		},
	}
}

// NopDescriptors registers the "Core" application backed by NopCore.
func NopDescriptors() monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			CoreType: monstera.CoreTypeInMemory,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NopCore{}
			},
		},
	}
}
