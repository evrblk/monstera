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

// GrpcTransport is the gRPC-backed implementation of transport.Transport.
type GrpcTransport struct {
	clusterConfig *cluster.Config
	pool          *GrpcClientPool[monsterapb.MonsteraApiClient]

	streamsMu sync.Mutex
	streams   map[string]*raftMessageStream
}

var _ transport.Transport = &GrpcTransport{}

func NewGrpcTransport(clusterConfig *cluster.Config) *GrpcTransport {
	return &GrpcTransport{
		clusterConfig: clusterConfig,
		pool: NewGrpcClientPool[monsterapb.MonsteraApiClient](func(conn *grpc.ClientConn) monsterapb.MonsteraApiClient {
			return monsterapb.NewMonsteraApiClient(conn)
		}),
		streams: make(map[string]*raftMessageStream),
	}
}

func (t *GrpcTransport) getConnection(nodeId string) (monsterapb.MonsteraApiClient, error) {
	node, err := t.clusterConfig.GetNode(nodeId)
	if err != nil {
		return nil, fmt.Errorf("clusterConfig.GetNode: %v", err)
	}

	conn, err := t.pool.GetConnection(node.GrpcAddress)
	if err != nil {
		return nil, fmt.Errorf("pool.GetConnection: %v", err)
	}

	return conn, nil
}

func (t *GrpcTransport) getOrCreateStream(nodeId string) (*raftMessageStream, error) {
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

func (t *GrpcTransport) HealthCheck(ctx context.Context, nodeId string) ([]*transport.ReplicaState, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.HealthCheck(ctx, &monsterapb.HealthCheckRequest{})
	if err != nil {
		return nil, err
	}

	states := make([]*transport.ReplicaState, len(resp.Replicas))
	for i, r := range resp.Replicas {
		var raftState transport.RaftState
		switch s := r.RaftState; s {
		case monsterapb.RaftState_RAFT_STATE_FOLLOWER:
			raftState = transport.RaftStateFollower
		case monsterapb.RaftState_RAFT_STATE_CANDIDATE:
			raftState = transport.RaftStateCandidate
		case monsterapb.RaftState_RAFT_STATE_LEADER:
			raftState = transport.RaftStateLeader
		case monsterapb.RaftState_RAFT_STATE_SHUTDOWN:
			raftState = transport.RaftStateDead
		default:
			return nil, fmt.Errorf("unknown raft state: %v", s)
		}

		states[i] = &transport.ReplicaState{
			ReplicaId: r.ReplicaId,
			RaftState: raftState,
		}
	}
	return states, nil
}

func (t *GrpcTransport) Read(ctx context.Context, nodeId string, request *transport.ReadRequest) (*transport.ReadResponse, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Read(ctx, &monsterapb.ReadRequest{
		Payload:                request.Payload,
		ShardKey:               request.ShardKey,
		ApplicationName:        request.ApplicationName,
		ShardId:                request.ShardId,
		ReplicaId:              request.ReplicaId,
		AllowReadFromFollowers: request.AllowReadFromFollowers,
		Hops:                   request.Hops,
	})
	if err != nil {
		return nil, err
	}

	return &transport.ReadResponse{
		Payload: resp.Payload,
	}, nil
}

func (t *GrpcTransport) Update(ctx context.Context, nodeId string, request *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := conn.Update(ctx, &monsterapb.UpdateRequest{
		Payload:         request.Payload,
		ShardKey:        request.ShardKey,
		ApplicationName: request.ApplicationName,
		ShardId:         request.ShardId,
		ReplicaId:       request.ReplicaId,
		Hops:            request.Hops,
	})
	if err != nil {
		return nil, err
	}

	return &transport.UpdateResponse{
		Payload: resp.Payload,
	}, nil
}

func (t *GrpcTransport) RaftMessage(ctx context.Context, nodeId string, request *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if nodeId == "" {
		panic(fmt.Errorf("nodeId is required"))
	}

	if request.ReplicaId == "" {
		panic(fmt.Errorf("replicaId is required"))
	}

	s, err := t.getOrCreateStream(nodeId)
	if err != nil {
		return nil, err
	}

	resp, err := s.send(ctx, &monsterapb.RaftMessageRequest{
		ReplicaId:   request.ReplicaId,
		MessageType: request.MessageType,
		Message:     request.Message,
	})
	if err != nil {
		return nil, err
	}

	return &transport.RaftMessageResponse{
		MessageType: resp.MessageType,
		Message:     resp.Message,
	}, nil
}

func (t *GrpcTransport) Close() {
	t.streamsMu.Lock()
	for _, s := range t.streams {
		s.cancel()
	}
	t.streamsMu.Unlock()

	t.pool.Close()
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
