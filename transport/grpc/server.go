package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

type GrpcServer struct {
	logger *log.Logger

	handler *handler
	lis     net.Listener
	srv     *grpc.Server
}

func (s *GrpcServer) Serve(address string) error {
	s.logger.Printf("Starting gRPC server")

	lis, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	s.lis = lis

	s.srv = grpc.NewServer()
	monsterapb.RegisterMonsteraApiServer(s.srv, s.handler)

	return s.srv.Serve(lis)
}

func (s *GrpcServer) Stop() {
	s.logger.Printf("Stopping gRPC server")

	s.srv.GracefulStop()
}

func NewGrpcServer(node *monstera.Node) *GrpcServer {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", node.NodeId()), log.LstdFlags)

	return &GrpcServer{
		handler: &handler{
			monsteraNode: node,
			logger:       logger,
		},
		logger: logger,
	}
}

type handler struct {
	monsterapb.UnimplementedMonsteraApiServer

	monsteraNode *monstera.Node
	logger       *log.Logger
}

var _ monsterapb.MonsteraApiServer = &handler{}

func (h *handler) Read(ctx context.Context, req *monsterapb.ReadRequest) (*monsterapb.ReadResponse, error) {
	resp, err := h.monsteraNode.Read(ctx, &transport.ReadRequest{
		ApplicationName:        req.ApplicationName,
		ShardId:                req.ShardId,
		ShardKey:               req.ShardKey,
		Payload:                req.Payload,
		AllowReadFromFollowers: req.AllowReadFromFollowers,
		Hops:                   req.Hops,
	})
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.Read: %v", err)
		return nil, err
	}

	return &monsterapb.ReadResponse{
		Payload: resp.Payload,
	}, nil
}

func (h *handler) Update(ctx context.Context, req *monsterapb.UpdateRequest) (*monsterapb.UpdateResponse, error) {
	resp, err := h.monsteraNode.Update(ctx, &transport.UpdateRequest{
		ApplicationName: req.ApplicationName,
		ShardId:         req.ShardId,
		ShardKey:        req.ShardKey,
		Payload:         req.Payload,
		Hops:            req.Hops,
	})
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.Update: %v", err)
		return nil, err
	}

	return &monsterapb.UpdateResponse{
		Payload: resp.Payload,
	}, nil
}

func (h *handler) TriggerSnapshot(ctx context.Context, req *monsterapb.TriggerSnapshotRequest) (*monsterapb.TriggerSnapshotResponse, error) {
	err := h.monsteraNode.TriggerSnapshot(req.ReplicaId)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.TriggerSnapshot: %v", err)
		return nil, err
	}
	return &monsterapb.TriggerSnapshotResponse{}, nil
}

func (h *handler) LeadershipTransfer(ctx context.Context, req *monsterapb.LeadershipTransferRequest) (*monsterapb.LeadershipTransferResponse, error) {
	err := h.monsteraNode.LeadershipTransfer(req.ReplicaId)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.LeadershipTransfer: %v", err)
		return nil, err
	}
	return &monsterapb.LeadershipTransferResponse{}, nil
}

func (h *handler) HealthCheck(ctx context.Context, req *monsterapb.HealthCheckRequest) (*monsterapb.HealthCheckResponse, error) {
	cores := h.monsteraNode.ListReplicas()

	replicas := make([]*monsterapb.ReplicaState, len(cores))
	for i, c := range cores {
		snapshots, err := c.ListSnapshots()
		if err != nil {
			h.logger.Printf("Error calling MonsteraReplica.ListSnapshots: %v", err)
		}

		replicas[i] = &monsterapb.ReplicaState{
			ReplicaId: c.GetReplicaId(),
			RaftStats: encodeRaftStats(c.GetRaftStats()),
			Snapshots: encodeRaftSnapshots(snapshots),
		}
	}

	return &monsterapb.HealthCheckResponse{
		Replicas: replicas,
	}, nil
}

func (h *handler) UpdateClusterConfig(ctx context.Context, req *monsterapb.UpdateClusterConfigRequest) (*monsterapb.UpdateClusterConfigResponse, error) {
	err := h.monsteraNode.UpdateClusterConfig(ctx, req.Config)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.UpdateClusterConfig: %v", err)
		return nil, err
	}

	return &monsterapb.UpdateClusterConfigResponse{}, nil
}

func (h *handler) RaftMessage(stream grpc.BidiStreamingServer[monsterapb.RaftMessageRequest, monsterapb.RaftMessageResponse]) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		resp, err := h.monsteraNode.RaftMessage(stream.Context(), &transport.RaftMessageRequest{
			ReplicaId:   req.ReplicaId,
			MessageType: req.MessageType,
			Message:     req.Message,
		})
		if err != nil {
			h.logger.Printf("Error calling MonsteraNode.RaftMessage: %v", err)
			return err
		}

		if err := stream.Send(&monsterapb.RaftMessageResponse{
			MessageType:         resp.MessageType,
			Message:             resp.Message,
			ResponseToMessageId: req.MessageId,
		}); err != nil {
			return err
		}
	}
}

// encodeRaftStats renders the structured Raft stats onto the wire message. The
// field set is Monstera's own contract, independent of the underlying Raft
// library.
func encodeRaftStats(s raft.RaftStats) *monsterapb.RaftStats {
	return &monsterapb.RaftStats{
		State:             encodeRaftState(s.State),
		Term:              s.Term,
		LastLogIndex:      s.LastLogIndex,
		LastLogTerm:       s.LastLogTerm,
		CommitIndex:       s.CommitIndex,
		AppliedIndex:      s.AppliedIndex,
		FsmPending:        s.FSMPending,
		LastSnapshotIndex: s.LastSnapshotIndex,
		LastSnapshotTerm:  s.LastSnapshotTerm,
		NumPeers:          int32(s.NumPeers),
		LastContactNanos:  int64(s.LastContact),
	}
}

func encodeRaftSnapshots(s []raft.SnapshotMetadata) []*monsterapb.RaftSnapshot {
	ret := make([]*monsterapb.RaftSnapshot, len(s))
	for i, s := range s {
		ret[i] = &monsterapb.RaftSnapshot{
			Id:    s.Id,
			Index: s.Index,
			Term:  s.Term,
			Size:  s.Size,
		}
	}
	return ret
}

func encodeRaftState(s raft.RaftState) monsterapb.RaftState {
	switch s {
	case raft.Follower:
		return monsterapb.RaftState_RAFT_STATE_FOLLOWER
	case raft.Candidate:
		return monsterapb.RaftState_RAFT_STATE_CANDIDATE
	case raft.Shutdown:
		return monsterapb.RaftState_RAFT_STATE_SHUTDOWN
	case raft.Leader:
		return monsterapb.RaftState_RAFT_STATE_LEADER
	default:
		panic(fmt.Sprintf("Unknown enum value %v", s))
	}
}
