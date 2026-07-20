package testcore

import (
	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
)

// PlaygroundDescriptors registers the "Core" application backed by PlaygroundCore.
func PlaygroundDescriptors() monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			RestoreSnapshotOnStart: false,
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NewPlaygroundCore()
			},
		},
	}
}

// NopDescriptors registers the "Core" application backed by NopCore.
func NopDescriptors() monstera.ApplicationCoreDescriptors {
	return monstera.ApplicationCoreDescriptors{
		"Core": {
			CoreFactoryFunc: func(shard *cluster.Shard, replica *cluster.Replica) monstera.ApplicationCore {
				return NopCore{}
			},
		},
	}
}
