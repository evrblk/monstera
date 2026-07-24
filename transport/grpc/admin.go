package grpc

import (
	"context"

	"google.golang.org/grpc"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// AdminClient is the gRPC-backed implementation of transport.AdminPlane. Unlike
// the data plane, it holds no cluster config: every method dials the target by its
// raw gRPC address. This is what lets config bootstrap and node provisioning run
// before any config is available. Connections are pooled by address; admin traffic
// is infrequent, so the pool stays small.
type AdminClient struct {
	pool *GrpcClientPool[monsterapb.MonsteraApiClient]
}

var _ transport.AdminPlane = &AdminClient{}

func NewAdminClient() *AdminClient {
	return &AdminClient{
		pool: NewGrpcClientPool[monsterapb.MonsteraApiClient](func(conn *grpc.ClientConn) monsterapb.MonsteraApiClient {
			return monsterapb.NewMonsteraApiClient(conn)
		}),
	}
}

func (a *AdminClient) Bootstrap(ctx context.Context, address string, nodeId string, config *cluster.Config) error {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return err
	}

	_, err = conn.Bootstrap(ctx, &monsterapb.BootstrapRequest{NodeId: nodeId, Config: config})
	return err
}

func (a *AdminClient) GetClusterConfig(ctx context.Context, address string) (*cluster.Config, error) {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return nil, err
	}

	resp, err := conn.GetClusterConfig(ctx, &monsterapb.GetClusterConfigRequest{})
	if err != nil {
		return nil, err
	}

	return resp.Config, nil
}

func (a *AdminClient) UpdateClusterConfig(ctx context.Context, address string, config *cluster.Config) error {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return err
	}

	_, err = conn.UpdateClusterConfig(ctx, &monsterapb.UpdateClusterConfigRequest{Config: config})
	return err
}

func (a *AdminClient) ListReplicaStates(ctx context.Context, address string) ([]*transport.ReplicaState, error) {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return nil, err
	}

	resp, err := conn.ListReplicaStates(ctx, &monsterapb.ListReplicaStatesRequest{})
	if err != nil {
		return nil, err
	}

	return decodeReplicaStates(resp)
}

func (a *AdminClient) ListReplicaSnapshots(ctx context.Context, address string, replicaId string) ([]*transport.RaftSnapshot, error) {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return nil, err
	}

	resp, err := conn.ListReplicaSnapshots(ctx, &monsterapb.ListReplicaSnapshotsRequest{ReplicaId: replicaId})
	if err != nil {
		return nil, err
	}

	return decodeRaftSnapshots(resp.Snapshots), nil
}

func (a *AdminClient) TriggerSnapshot(ctx context.Context, address string, replicaId string) error {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return err
	}

	_, err = conn.TriggerSnapshot(ctx, &monsterapb.TriggerSnapshotRequest{ReplicaId: replicaId})
	return err
}

func (a *AdminClient) LeadershipTransfer(ctx context.Context, address string, replicaId string) error {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return err
	}

	_, err = conn.LeadershipTransfer(ctx, &monsterapb.LeadershipTransferRequest{ReplicaId: replicaId})
	return err
}

// SplitCutoff proposes the shard-split CUTOFF through the shard's replica on
// the node at address (which must be the shard's Raft leader).
func (a *AdminClient) SplitCutoff(ctx context.Context, address string, shardId string) error {
	conn, err := a.pool.GetConnection(address)
	if err != nil {
		return err
	}
	_, err = conn.SplitCutoff(ctx, &monsterapb.SplitCutoffRequest{ShardId: shardId})
	return err
}

func (a *AdminClient) Close() error {
	a.pool.Close()
	return nil
}
