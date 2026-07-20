package grpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// DataPlaneClient is the gRPC-backed implementation of transport.DataPlane. It
// addresses nodes by nodeId, resolving nodeId -> gRPC address from the cluster
// config swapped in via SetClusterConfig, and owns the pooled connections and the
// persistent multiplexed Raft streams.
//
// It is constructed without a config: the config owner (a monstera.Node, or a
// monstera.Client via its config provider) pushes it in with SetClusterConfig.
// Until then, calls fail with a clear error rather than dereferencing a nil config.
type DataPlaneClient struct {
	// configMu guards clusterConfig, which is swapped by SetClusterConfig when the
	// cluster topology changes. It is nil until the owner pushes the first config.
	configMu      sync.RWMutex
	clusterConfig *cluster.Config

	pool *GrpcClientPool[monsterapb.MonsteraApiClient]

	streamsMu sync.Mutex
	streams   map[string]*raftMessageStream
}

var _ transport.DataPlane = &DataPlaneClient{}
var _ transport.ClusterConfigConsumer = &DataPlaneClient{}

func NewDataPlaneClient() *DataPlaneClient {
	return &DataPlaneClient{
		pool: NewGrpcClientPool[monsterapb.MonsteraApiClient](func(conn *grpc.ClientConn) monsterapb.MonsteraApiClient {
			return monsterapb.NewMonsteraApiClient(conn)
		}),
		streams: make(map[string]*raftMessageStream),
	}
}

// SetClusterConfig updates the transport's view of the cluster so it can resolve
// newly added nodes and stops pooling connections/streams to nodes that were
// removed or re-addressed. New nodes are dialed lazily on next use.
func (t *DataPlaneClient) SetClusterConfig(config *cluster.Config) {
	if config == nil {
		return
	}

	t.configMu.Lock()
	old := t.clusterConfig
	t.clusterConfig = config
	t.configMu.Unlock()

	newAddrs := make(map[string]bool, len(config.Nodes))
	newIds := make(map[string]bool, len(config.Nodes))
	for _, n := range config.Nodes {
		newAddrs[n.GrpcAddress] = true
		newIds[n.Id] = true
	}

	if old != nil {
		for _, n := range old.Nodes {
			if !newAddrs[n.GrpcAddress] {
				t.pool.DeleteConnection(n.GrpcAddress)
			}
		}
	}

	// Drop raft-message streams to nodes no longer in the config.
	t.streamsMu.Lock()
	for id, s := range t.streams {
		if !newIds[id] {
			s.cancel()
			delete(t.streams, id)
		}
	}
	t.streamsMu.Unlock()
}

func (t *DataPlaneClient) ListReplicaStates(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.ListReplicaStates(ctx, &monsterapb.ListReplicaStatesRequest{})
	if err != nil {
		return nil, err
	}

	return decodeReplicaStates(resp)
}

func (t *DataPlaneClient) Read(ctx context.Context, nodeId string, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Read(ctx, &monsterapb.ReadRequest{
		Payload:                req.Payload,
		ShardKey:               req.ShardKey,
		ApplicationName:        req.ApplicationName,
		ShardId:                req.ShardId,
		AllowReadFromFollowers: req.AllowReadFromFollowers,
		Hops:                   req.Hops,
	})
	if err != nil {
		return nil, err
	}

	return &transport.ReadResponse{
		Payload: resp.Payload,
	}, nil
}

func (t *DataPlaneClient) Update(ctx context.Context, nodeId string, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Update(ctx, &monsterapb.UpdateRequest{
		Payload:         req.Payload,
		ShardKey:        req.ShardKey,
		ApplicationName: req.ApplicationName,
		ShardId:         req.ShardId,
		Hops:            req.Hops,
	})
	if err != nil {
		return nil, err
	}

	return &transport.UpdateResponse{
		Payload: resp.Payload,
	}, nil
}

func (t *DataPlaneClient) RaftMessage(ctx context.Context, nodeId string, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if nodeId == "" {
		return nil, fmt.Errorf("nodeId is required")
	}

	if req.ReplicaId == "" {
		return nil, fmt.Errorf("replicaId is required")
	}

	s, err := t.getOrCreateStream(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := s.send(ctx, &monsterapb.RaftMessageRequest{
		ReplicaId:   req.ReplicaId,
		MessageType: req.MessageType,
		Message:     req.Message,
	})
	if err != nil {
		return nil, err
	}

	return &transport.RaftMessageResponse{
		MessageType: resp.MessageType,
		Message:     resp.Message,
	}, nil
}

func (t *DataPlaneClient) Close() error {
	t.streamsMu.Lock()
	for _, s := range t.streams {
		s.cancel()
	}
	t.streamsMu.Unlock()

	t.pool.Close()

	return nil
}

func (t *DataPlaneClient) getConnection(nodeId string) (monsterapb.MonsteraApiClient, error) {
	t.configMu.RLock()
	cfg := t.clusterConfig
	t.configMu.RUnlock()
	if cfg == nil {
		return nil, fmt.Errorf("no cluster config set on data plane")
	}

	node, err := cfg.GetNode(nodeId)
	if err != nil {
		return nil, fmt.Errorf("clusterConfig.GetNode: %v", err)
	}

	conn, err := t.pool.GetConnection(node.GrpcAddress)
	if err != nil {
		return nil, fmt.Errorf("pool.GetConnection: %v", err)
	}

	return conn, nil
}

func (t *DataPlaneClient) getOrCreateStream(nodeId string) (*raftMessageStream, error) {
	t.streamsMu.Lock()
	defer t.streamsMu.Unlock()

	s, ok := t.streams[nodeId]
	if ok {
		// Check if still alive.
		select {
		case <-s.dead:
			// Stream is dead; fall through to create a new one.
		default:
			return s, nil
		}
	}

	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	s, err = newRaftMessageStream(conn)
	if err != nil {
		return nil, err
	}
	t.streams[nodeId] = s
	return s, nil
}

// raftMessageStream manages a persistent bidirectional gRPC stream for raft messages.
type raftMessageStream struct {
	mu     sync.Mutex // serializes Send calls
	stream grpc.BidiStreamingClient[monsterapb.RaftMessageRequest, monsterapb.RaftMessageResponse]
	cancel context.CancelFunc
	nextID atomic.Int64

	pendingMu sync.Mutex
	pending   map[int64]chan *monsterapb.RaftMessageResponse

	dead    chan struct{}
	deadErr error
}

func newRaftMessageStream(conn monsterapb.MonsteraApiClient) (*raftMessageStream, error) {
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := conn.RaftMessage(ctx)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &raftMessageStream{
		stream:  stream,
		cancel:  cancel,
		pending: make(map[int64]chan *monsterapb.RaftMessageResponse),
		dead:    make(chan struct{}),
	}
	go s.recvLoop()
	return s, nil
}

func (s *raftMessageStream) recvLoop() {
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			s.deadErr = err
			close(s.dead)
			s.pendingMu.Lock()
			for _, ch := range s.pending {
				close(ch)
			}
			s.pending = nil
			s.pendingMu.Unlock()
			return
		}
		s.pendingMu.Lock()
		ch, ok := s.pending[resp.ResponseToMessageId]
		if ok {
			delete(s.pending, resp.ResponseToMessageId)
		}
		s.pendingMu.Unlock()
		if ok {
			ch <- resp
			close(ch)
		}
	}
}

func (s *raftMessageStream) send(ctx context.Context, req *monsterapb.RaftMessageRequest) (*monsterapb.RaftMessageResponse, error) {
	msgID := s.nextID.Add(1)
	req.MessageId = msgID

	ch := make(chan *monsterapb.RaftMessageResponse, 1)
	s.pendingMu.Lock()
	s.pending[msgID] = ch
	s.pendingMu.Unlock()

	s.mu.Lock()
	err := s.stream.Send(req)
	s.mu.Unlock()

	if err != nil {
		s.pendingMu.Lock()
		delete(s.pending, msgID)
		s.pendingMu.Unlock()
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, s.deadErr
		}
		return resp, nil
	case <-ctx.Done():
		s.pendingMu.Lock()
		delete(s.pending, msgID)
		s.pendingMu.Unlock()
		return nil, ctx.Err()
	case <-s.dead:
		return nil, s.deadErr
	}
}
