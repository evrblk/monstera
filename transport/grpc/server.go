package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/evrblk/monstera"
	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// defaultServerKeepaliveEnforcement bounds how aggressively clients may ping.
// MinTime must be <= the client's keepalive Time (DefaultClientKeepalive) or the
// server GOAWAYs the connection. PermitWithoutStream matches the client so probes
// during idle gaps between Raft streams are not treated as abuse.
var defaultServerKeepaliveEnforcement = keepalive.EnforcementPolicy{
	MinTime:             5 * time.Second,
	PermitWithoutStream: true,
}

// defaultServerKeepalive makes the server independently probe clients so a
// black-holed peer's half-open stream is reaped instead of pinning a handler
// goroutine forever.
var defaultServerKeepalive = keepalive.ServerParameters{
	Time:    10 * time.Second,
	Timeout: 3 * time.Second,
}

// serverOptions returns the gRPC server options shared by the production server
// and tests, so both exercise the same keepalive configuration.
func serverOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.KeepaliveEnforcementPolicy(defaultServerKeepaliveEnforcement),
		grpc.KeepaliveParams(defaultServerKeepalive),
	}
}

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

	s.srv = grpc.NewServer(serverOptions()...)
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

// node is the subset of *monstera.Node that the gRPC handler depends on. Used for tests.
type node interface {
	Read(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error)
	Update(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error)
	RaftMessage(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error)
	TriggerSnapshot(replicaId string) error
	LeadershipTransfer(replicaId string) error
	SplitCutoff(ctx context.Context, shardId string) (uint64, error)
	ReplicaStates() []*transport.ReplicaState
	ListSnapshots(replicaId string) ([]raft.SnapshotMetadata, error)
	UpdateClusterConfig(ctx context.Context, config *cluster.Config) error
	GetClusterConfig() *cluster.Config
	Bootstrap(ctx context.Context, nodeId string, config *cluster.Config) error
}

type handler struct {
	monsterapb.UnimplementedMonsteraApiServer

	monsteraNode node
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

// RaftMessage serves the persistent, bidirectional stream over which the client
// multiplexes Raft traffic for every replica pair between two nodes, correlating
// responses by MessageId (so the client already tolerates out-of-order replies).
//
// Each message is handled in its own goroutine: handling one message can block
// for a long time — appendEntries waits on disk I/O, and the final snapshot chunk
// blocks on an entire FSM restore — so a single reader loop that did Recv →
// handle → Send would let those stalls starve heartbeats for every other replica
// sharing the stream, tripping their election timeouts. Recv stays in this one
// reader loop; only stream.Send is serialized (concurrent Send is not allowed).
func (h *handler) RaftMessage(stream grpc.BidiStreamingServer[monsterapb.RaftMessageRequest, monsterapb.RaftMessageResponse]) error {
	var (
		sendMu   sync.Mutex
		wg       sync.WaitGroup
		errOnce  sync.Once
		fatalErr error
	)
	setFatal := func(err error) {
		errOnce.Do(func() { fatalErr = err })
	}

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			setFatal(err)
			break
		}

		wg.Add(1)
		go func(req *monsterapb.RaftMessageRequest) {
			defer wg.Done()

			var out *monsterapb.RaftMessageResponse
			resp, err := h.monsteraNode.RaftMessage(stream.Context(), &transport.RaftMessageRequest{
				ReplicaId:   req.ReplicaId,
				MessageType: req.MessageType,
				Message:     req.Message,
			})
			if err != nil {
				// A per-message failure (e.g. an unknown replica or a not-ready
				// node) concerns only this message and its target replica. Return it
				// to the caller as an error envelope correlated by MessageId rather
				// than tearing down the stream that every other replica pair shares;
				// only transport-level Send/Recv errors are fatal to the stream.
				h.logger.Printf("Error calling MonsteraNode.RaftMessage: %v", err)
				out = &monsterapb.RaftMessageResponse{
					ResponseToMessageId: req.MessageId,
					Error:               err.Error(),
				}
			} else {
				out = &monsterapb.RaftMessageResponse{
					MessageType:         resp.MessageType,
					Message:             resp.Message,
					ResponseToMessageId: req.MessageId,
				}
			}

			sendMu.Lock()
			defer sendMu.Unlock()
			if err := stream.Send(out); err != nil {
				setFatal(err)
			}
		}(req)
	}

	// gRPC forbids Send once the handler returns, so wait for the in-flight
	// handlers (which may still be mid-Send) to drain before returning.
	wg.Wait()
	return fatalErr
}
