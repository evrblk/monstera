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
		log.Printf("Error calling MonsteraNode.Read: %v", err)
		return nil, err
	}

	return &ReadResponse{
		Payload: payload,
	}, nil
}

func (s *MonsteraServer) Update(ctx context.Context, request *UpdateRequest) (*UpdateResponse, error) {
	payload, err := s.monsteraNode.Update(request)
	if err != nil {
		log.Printf("Error calling MonsteraNode.Update: %v", err)
		return nil, err
	}

	return &UpdateResponse{
		Payload: payload,
	}, nil
}

func (s *MonsteraServer) AddVoter(ctx context.Context, request *AddVoterRequest) (*AddVoterResponse, error) {
	err := s.monsteraNode.AddVoter(request.ReplicaId, request.VoterReplicaId, request.VoterAddress)
	if err != nil {
		log.Printf("Error calling MonsteraNode.AddVoter: %v", err)
		return nil, err
	}

	return &AddVoterResponse{}, nil
}

func (s *MonsteraServer) AppendEntries(ctx context.Context, request *AppendEntriesRequest) (*AppendEntriesResponse, error) {
	resp, err := s.monsteraNode.AppendEntries(request.TargetReplicaId, decodeAppendEntriesRequest(request))
	if err != nil {
		log.Printf("Error calling MonsteraNode.AppendEntries: %v", err)
		return nil, err
	}
	return encodeAppendEntriesResponse(resp), nil
}

func (s *MonsteraServer) RequestVote(ctx context.Context, request *RequestVoteRequest) (*RequestVoteResponse, error) {
	resp, err := s.monsteraNode.RequestVote(request.TargetReplicaId, decodeRequestVoteRequest(request))
	if err != nil {
		log.Printf("Error calling MonsteraNode.RequestVote: %v", err)
		return nil, err
	}
	return encodeRequestVoteResponse(resp), nil
}

func (s *MonsteraServer) TimeoutNow(ctx context.Context, request *TimeoutNowRequest) (*TimeoutNowResponse, error) {
	resp, err := s.monsteraNode.TimeoutNow(request.TargetReplicaId, decodeTimeoutNowRequest(request))
	if err != nil {
		log.Printf("Error calling MonsteraNode.TimeoutNow: %v", err)
		return nil, err
	}
	return encodeTimeoutNowResponse(resp), nil
}

func (s *MonsteraServer) InstallSnapshot(stream MonsteraApi_InstallSnapshotServer) error {
	request, err := stream.Recv()
	if err != nil {
		log.Printf("Error calling stream.Recv: %v", err)
		return err
	}

	resp, err := s.monsteraNode.InstallSnapshot(request.TargetReplicaId, decodeInstallSnapshotRequest(request), &snapshotStream{stream, request.GetData()})
	if err != nil {
		log.Printf("Error calling MonsteraNode.InstallSnapshot: %v", err)
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
			// TODO(quis): One failure doesn't have to break the entire stream?
			// Or does it all go wrong when it's out of order anyway?
			return err
		}
		if err := stream.Send(encodeAppendEntriesResponse(resp)); err != nil {
			return err
		}
	}
}

func (s *MonsteraServer) HealthCheck(ctx context.Context, request *HealthCheckRequest) (*HealthCheckResponse, error) {
	cores := s.monsteraNode.ListCores()

	replicas := make([]*ReplicaState, len(cores))
	for i, b := range cores {
		replicas[i] = &ReplicaState{
			ReplicaId: b.ReplicaId,
			RaftState: encodeRaftState(b.GetRaftState()),
			RaftStats: b.GetRaftStats(),
		}
	}

	return &HealthCheckResponse{
		Replicas: replicas,
	}, nil
}

func (s *MonsteraServer) UpdateClusterConfig(ctx context.Context, request *UpdateClusterConfigRequest) (*UpdateClusterConfigResponse, error) {
	err := s.monsteraNode.UpdateClusterConfig(request.Config)
	if err != nil {
		log.Printf("Error calling MonsteraNode.UpdateClusterConfig: %v", err)
		return nil, err
	}

	return &UpdateClusterConfigResponse{}, nil
}

func NewMonsteraServer(monsteraNode *MonsteraNode) *MonsteraServer {
	return &MonsteraServer{
		monsteraNode: monsteraNode,
	}
}
