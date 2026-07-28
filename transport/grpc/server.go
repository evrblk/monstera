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
		return err
	}
	s.lis = lis

	s.srv = grpc.NewServer()
	monsterapb.RegisterMonsteraApiServer(s.srv, s.handler)

	return s.srv.Serve(lis)
}

func (s *GrpcServer) Stop() {
	s.logger.Printf("Stopping gRPC server")

	if s.srv != nil {
		s.srv.GracefulStop()
	}
}

// Kill stops the server immediately, closing the listener and every live
// connection and stream. Unlike Stop it does not wait for in-flight RPCs —
// peers holding persistent Raft streams open would block a graceful stop
// indefinitely — so this is the crash-like teardown for failure testing and
// for taking one node down while the rest of the cluster keeps running.
func (s *GrpcServer) Kill() {
	s.logger.Printf("Killing gRPC server")

	if s.srv != nil {
		s.srv.Stop()
	}
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
	shardKey, hasShardKey := decodeShardKey(req.ShardKey)
	resp, err := h.monsteraNode.Read(ctx, &transport.ReadRequest{
		ApplicationName:        req.ApplicationName,
		ShardId:                req.ShardId,
		ShardKey:               shardKey,
		HasShardKey:            hasShardKey,
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
	shardKey, hasShardKey := decodeShardKey(req.ShardKey)
	resp, err := h.monsteraNode.Update(ctx, &transport.UpdateRequest{
		ApplicationName: req.ApplicationName,
		ShardId:         req.ShardId,
		ShardKey:        shardKey,
		HasShardKey:     hasShardKey,
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

// SplitCutoff proposes the shard-split CUTOFF through this node's replica of
// the shard (which must be the Raft leader).
func (h *handler) SplitCutoff(ctx context.Context, req *monsterapb.SplitCutoffRequest) (*monsterapb.SplitCutoffResponse, error) {
	index, err := h.monsteraNode.SplitCutoff(ctx, req.ShardId)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.SplitCutoff: %v", err)
		return nil, err
	}
	return &monsterapb.SplitCutoffResponse{CutoffIndex: index}, nil
}

// ListReplicaStates returns the in-memory Raft state of every replica on this
// node. It is the lightweight, frequently-polled call the Monstera client uses
// to locate leaders; it does no disk I/O. Snapshot listing is a separate,
// on-demand call (ListReplicaSnapshots).
func (h *handler) ListReplicaStates(ctx context.Context, req *monsterapb.ListReplicaStatesRequest) (*monsterapb.ListReplicaStatesResponse, error) {
	return &monsterapb.ListReplicaStatesResponse{
		ReplicaStates: encodeReplicaStates(h.monsteraNode.ReplicaStates()),
	}, nil
}

// ListReplicaSnapshots returns the snapshots stored for a single replica on this
// node. It reads the replica's snapshot store from disk, so it is meant for
// on-demand admin/ops use, not for frequent polling.
func (h *handler) ListReplicaSnapshots(ctx context.Context, req *monsterapb.ListReplicaSnapshotsRequest) (*monsterapb.ListReplicaSnapshotsResponse, error) {
	snapshots, err := h.monsteraNode.ListSnapshots(req.ReplicaId)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.ListSnapshots: %v", err)
		return nil, err
	}

	return &monsterapb.ListReplicaSnapshotsResponse{
		Snapshots: encodeRaftSnapshots(snapshots),
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

// GetClusterConfig returns the cluster config this node is currently running
// with (including its version), for inspecting config rollout across the cluster.
// An UNPROVISIONED node has no config yet and returns an error rather than a nil
// config, so pollers cannot mistake "not bootstrapped" for a valid answer.
func (h *handler) GetClusterConfig(ctx context.Context, req *monsterapb.GetClusterConfigRequest) (*monsterapb.GetClusterConfigResponse, error) {
	config := h.monsteraNode.GetClusterConfig()
	if config == nil {
		return nil, fmt.Errorf("node is not provisioned: no cluster config")
	}
	return &monsterapb.GetClusterConfigResponse{
		Config: config,
	}, nil
}

// Bootstrap provisions an unprovisioned node with its id and initial cluster config.
func (h *handler) Bootstrap(ctx context.Context, req *monsterapb.BootstrapRequest) (*monsterapb.BootstrapResponse, error) {
	err := h.monsteraNode.Bootstrap(ctx, req.NodeId, req.Config)
	if err != nil {
		h.logger.Printf("Error calling MonsteraNode.Bootstrap: %v", err)
		return nil, err
	}
	return &monsterapb.BootstrapResponse{}, nil
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
