package monstera

import (
	"context"
	"log"
)

type MonsteraServer struct {
	UnimplementedMonsteraApiServer

	monsteraNode *MonsteraNode
}

var _ MonsteraApiServer = &MonsteraServer{}

func (s *MonsteraServer) Read(ctx context.Context, request *ReadRequest) (*ReadResponse, error) {
	payload, err := s.monsteraNode.Read(request)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.Read: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}

	return &ReadResponse{
		Payload: payload,
	}, nil
}

func (s *MonsteraServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	payload, err := s.monsteraNode.Update(request)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.Update: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}

	return &UpdateResponse{
		Payload: payload,
	}, nil
}

func (s *MonsteraServer) AddVoter(ctx context.Context, request *AddVoterRequest) (*AddVoterResponse, error) {
	err := s.monsteraNode.AddVoter(request.ReplicaId, request.VoterReplicaId, request.VoterAddress)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.AddVoter: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}

	return &AddVoterResponse{}, nil
}

func (s *MonsteraServer) AppendEntries(ctx context.Context, request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp, err := s.monsteraNode.AppendEntries(request.TargetReplicaId, decodeAppendEntriesRequest(request))
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.AppendEntries: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}
	return encodeAppendEntriesResponse(resp), nil
}

func (s *MonsteraServer) RequestVote(ctx context.Context, request *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp, err := s.monsteraNode.RequestVote(request.TargetReplicaId, decodeRequestVoteRequest(request))
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.RequestVote: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}
	return encodeRequestVoteResponse(resp), nil
}

func (s *MonsteraServer) TimeoutNow(ctx context.Context, request *TimeoutNowRequest) (*TimeoutNowResponse, error) {
	resp, err := s.monsteraNode.TimeoutNow(request.TargetReplicaId, decodeTimeoutNowRequest(request))
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.TimeoutNow: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}
	return encodeTimeoutNowResponse(resp), nil
}

func (s *MonsteraServer) InstallSnapshot(stream MonsteraApi_InstallSnapshotServer) error {
	request, err := stream.Recv()
	if err != nil {
		log.Printf("[%s] Error calling stream.Recv: %v", s.monsteraNode.nodeId, err)
		return err
	}

	resp, err := s.monsteraNode.InstallSnapshot(request.TargetReplicaId, decodeInstallSnapshotRequest(request), &snapshotStream{stream, request.GetData()})
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.InstallSnapshot: %v", s.monsteraNode.nodeId, err)
		return err
	}
	return stream.SendAndClose(encodeInstallSnapshotResponse(resp))
}

type snapshotStream struct {
	s MonsteraApi_InstallSnapshotServer

	buf []byte
}

func (s *snapshotStream) Read(b []byte) (int, error) {
	if len(s.buf) > 0 {
		n := copy(b, s.buf)
		s.buf = s.buf[n:]
		return n, nil
	}
	m, err := s.s.Recv()
	if err != nil {
		return 0, err
	}
	n := copy(b, m.GetData())
	if n < len(m.GetData()) {
		s.buf = m.GetData()[n:]
	}
	return n, nil
}

func (s *MonsteraServer) AppendEntriesPipeline(stream MonsteraApi_AppendEntriesPipelineServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		resp, err := s.monsteraNode.AppendEntries(msg.TargetReplicaId, decodeAppendEntriesRequest(msg))

		if err != nil {
			log.Printf("[%s] Error calling MonsteraNode.AppendEntries: %v", s.monsteraNode.nodeId, err)
			return err
		}
		if err := stream.Send(encodeAppendEntriesResponse(resp)); err != nil {
			return err
		}
	}
}

func (s *MonsteraServer) TriggerSnapshot(ctx context.Context, request *TriggerSnapshotRequest) (*TriggerSnapshotResponse, error) {
	err := s.monsteraNode.TriggerSnapshot(request.ReplicaId)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.TriggerSnapshot: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}
	return &TriggerSnapshotResponse{}, nil
}

func (s *MonsteraServer) LeadershipTransfer(ctx context.Context, request *LeadershipTransferRequest) (*LeadershipTransferResponse, error) {
	err := s.monsteraNode.LeadershipTransfer(request.ReplicaId)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.LeadershipTransfer: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}
	return &LeadershipTransferResponse{}, nil
}

func (s *MonsteraServer) HealthCheck(ctx context.Context, request *HealthCheckRequest) (*HealthCheckResponse, error) {
	cores := s.monsteraNode.ListCores()

	replicas := make([]*ReplicaState, len(cores))
	for i, c := range cores {
		snapshots, err := c.ListSnapshots()
		if err != nil {
			log.Printf("[%s] Error calling MonsteraReplica.ListSnapshots: %v", c.ReplicaId, err)
		}

		replicas[i] = &ReplicaState{
			ReplicaId: c.ReplicaId,
			RaftState: encodeRaftState(c.GetRaftState()),
			RaftStats: c.GetRaftStats(),
			Snapshots: encodeRaftSnapshots(snapshots),
		}
	}

	return &HealthCheckResponse{
		Replicas: replicas,
	}, nil
}

func (s *MonsteraServer) UpdateClusterConfig(ctx context.Context, request *UpdateClusterConfigRequest) (*UpdateClusterConfigResponse, error) {
	err := s.monsteraNode.UpdateClusterConfig(request.Config)
	if err != nil {
		log.Printf("[%s] Error calling MonsteraNode.UpdateClusterConfig: %v", s.monsteraNode.nodeId, err)
		return nil, err
	}

	return &UpdateClusterConfigResponse{}, nil
}

func NewMonsteraServer(monsteraNode *MonsteraNode) *MonsteraServer {
	return &MonsteraServer{
		monsteraNode: monsteraNode,
	}
}
