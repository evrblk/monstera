package raft

import (
	"context"
	"io"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"

	"github.com/evrblk/monstera/internal/raft/raftpb"
	"github.com/evrblk/monstera/transport"
)

type MessageType = int32

const (
	AppendEntriesRequest         MessageType = 1
	AppendEntriesResponse        MessageType = 2
	RequestVoteRequest           MessageType = 3
	RequestVoteResponse          MessageType = 4
	TimeoutNowRequest            MessageType = 5
	TimeoutNowResponse           MessageType = 6
	InstallSnapshotInitRequest   MessageType = 7
	InstallSnapshotInitResponse  MessageType = 8
	InstallSnapshotChunkRequest  MessageType = 9
	InstallSnapshotChunkResponse MessageType = 10
)

// RaftTransport implements [hraft.Transport].
// All outbound Raft RPCs are dispatched through Monstera [transport.Transport].
type RaftTransport struct {
	rpcChan         chan hraft.RPC
	localAddress    hraft.ServerAddress
	heartbeatFunc   func(hraft.RPC)
	heartbeatFuncMu sync.Mutex

	trans transport.Transport
}

var _ hraft.Transport = &RaftTransport{}

func (r *RaftTransport) Producer() chan hraft.RPC {
	return r.rpcChan
}

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (r *RaftTransport) Consumer() <-chan hraft.RPC {
	return r.rpcChan
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (r *RaftTransport) LocalAddr() hraft.ServerAddress {
	return r.localAddress
}

// AppendEntries sends the appropriate RPC to the target node.
func (r *RaftTransport) AppendEntries(id hraft.ServerID, target hraft.ServerAddress, args *hraft.AppendEntriesRequest, resp *hraft.AppendEntriesResponse) error {
	msg := encodeAppendEntriesRequest(args)
	data, err := msg.MarshalVT()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ret, err := r.trans.RaftMessage(ctx, string(target), &transport.RaftMessageRequest{
		Message:     data,
		MessageType: AppendEntriesRequest,
		ReplicaId:   string(id),
	})
	if err != nil {
		return err
	}

	respData := &raftpb.AppendEntriesResponse{}
	if err := respData.UnmarshalVT(ret.Message); err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(respData)

	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (r *RaftTransport) RequestVote(id hraft.ServerID, target hraft.ServerAddress, args *hraft.RequestVoteRequest, resp *hraft.RequestVoteResponse) error {
	msg := encodeRequestVoteRequest(args)
	data, err := msg.MarshalVT()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ret, err := r.trans.RaftMessage(ctx, string(target), &transport.RaftMessageRequest{
		Message:     data,
		MessageType: RequestVoteRequest,
		ReplicaId:   string(id),
	})
	if err != nil {
		return err
	}

	respData := &raftpb.RequestVoteResponse{}
	if err := respData.UnmarshalVT(ret.Message); err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(respData)

	return nil
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (r *RaftTransport) TimeoutNow(id hraft.ServerID, target hraft.ServerAddress, args *hraft.TimeoutNowRequest, resp *hraft.TimeoutNowResponse) error {
	msg := encodeTimeoutNowRequest(args, string(id))
	data, err := msg.MarshalVT()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	ret, err := r.trans.RaftMessage(ctx, string(target), &transport.RaftMessageRequest{
		Message:     data,
		MessageType: TimeoutNowRequest,
		ReplicaId:   string(id),
	})
	if err != nil {
		return err
	}

	respData := &raftpb.TimeoutNowResponse{}
	if err := respData.UnmarshalVT(ret.Message); err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(respData)
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and sent to the client.
func (r *RaftTransport) InstallSnapshot(id hraft.ServerID, target hraft.ServerAddress, req *hraft.InstallSnapshotRequest, resp *hraft.InstallSnapshotResponse, data io.Reader) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute) // TODO: configurable timeout
	defer cancel()

	msgData, err := encodeInstallSnapshotInitRequest(req).MarshalVT()
	if err != nil {
		return err
	}
	lastRet, err := r.trans.RaftMessage(ctx, string(target), &transport.RaftMessageRequest{
		Message:     msgData,
		MessageType: InstallSnapshotInitRequest,
		ReplicaId:   string(id),
	})
	if err != nil {
		return err
	}

	var buf [16384]byte
	for {
		n, err := data.Read(buf[:])
		if err == io.EOF || (err == nil && n == 0) {
			break
		}
		if err != nil {
			return err
		}
		chunkData, err := (&raftpb.InstallSnapshotChunkRequest{Data: buf[:n]}).MarshalVT()
		if err != nil {
			return err
		}
		lastRet, err = r.trans.RaftMessage(ctx, string(target), &transport.RaftMessageRequest{
			Message:     chunkData,
			MessageType: InstallSnapshotChunkRequest,
			ReplicaId:   string(id),
		})
		if err != nil {
			return err
		}
		// TODO: check lastRet message type is ack
	}

	// TODO: check lastRet message type is InstallSnapshotResponse
	respData := &raftpb.InstallSnapshotChunkResponse{}
	if err := respData.UnmarshalVT(lastRet.Message); err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotChunkResponse(respData)
	return nil
}

// AppendEntriesPipeline returns an interface that can be used to pipeline AppendEntries requests.
func (r *RaftTransport) AppendEntriesPipeline(id hraft.ServerID, target hraft.ServerAddress) (hraft.AppendPipeline, error) {
	ctx, cancel := context.WithCancel(context.Background())
	rpa := &raftPipelineAPI{
		ctx:             ctx,
		cancel:          cancel,
		inflightCh:      make(chan *appendFuture, 20),
		doneCh:          make(chan hraft.AppendFuture, 20),
		targetReplicaId: string(id),
		targetNodeId:    string(target),
		trans:           r.trans,
	}
	go rpa.receiver()
	return rpa, nil
}

type raftPipelineAPI struct {
	ctx             context.Context
	cancel          func()
	inflightChMtx   sync.Mutex
	inflightCh      chan *appendFuture
	doneCh          chan hraft.AppendFuture
	targetReplicaId string
	targetNodeId    string
	trans           transport.Transport
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (r *raftPipelineAPI) AppendEntries(req *hraft.AppendEntriesRequest, resp *hraft.AppendEntriesResponse) (hraft.AppendFuture, error) {
	data, err := encodeAppendEntriesRequest(req).MarshalVT()
	if err != nil {
		return nil, err
	}

	af := &appendFuture{
		start:   time.Now(),
		request: req,
		data:    data,
		done:    make(chan struct{}),
	}

	r.inflightChMtx.Lock()
	select {
	case <-r.ctx.Done():
		r.inflightChMtx.Unlock()
		return nil, r.ctx.Err()
	default:
		r.inflightCh <- af
	}
	r.inflightChMtx.Unlock()

	return af, nil
}

// Consumer returns a channel that can be used to consume
// response futures when they are ready.
func (r *raftPipelineAPI) Consumer() <-chan hraft.AppendFuture {
	return r.doneCh
}

// Close closes the pipeline and cancels all inflight RPCs
func (r *raftPipelineAPI) Close() error {
	r.cancel()
	r.inflightChMtx.Lock()
	close(r.inflightCh)
	r.inflightChMtx.Unlock()
	return nil
}

func (r *raftPipelineAPI) receiver() {
	for af := range r.inflightCh {
		ret, err := r.trans.RaftMessage(r.ctx, r.targetNodeId, &transport.RaftMessageRequest{
			Message:     af.data,
			MessageType: AppendEntriesRequest,
			ReplicaId:   r.targetReplicaId,
		})
		if err != nil {
			af.err = err
		} else {
			respData := &raftpb.AppendEntriesResponse{}
			if err := respData.UnmarshalVT(ret.Message); err != nil {
				af.err = err
			} else {
				af.response = *decodeAppendEntriesResponse(respData)
			}
		}
		close(af.done)
		r.doneCh <- af
	}
}

type appendFuture struct {
	hraft.AppendFuture

	start    time.Time
	request  *hraft.AppendEntriesRequest
	data     []byte
	response hraft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

func (f *appendFuture) Start() time.Time {
	return f.start
}

func (f *appendFuture) Request() *hraft.AppendEntriesRequest {
	return f.request
}

func (f *appendFuture) Response() *hraft.AppendEntriesResponse {
	return &f.response
}

// EncodePeer is used to serialize a peer's address.
func (r *RaftTransport) EncodePeer(id hraft.ServerID, addr hraft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (r *RaftTransport) DecodePeer(p []byte) hraft.ServerAddress {
	return hraft.ServerAddress(p)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO.
func (r *RaftTransport) SetHeartbeatHandler(cb func(rpc hraft.RPC)) {
	r.heartbeatFuncMu.Lock()
	r.heartbeatFunc = cb
	r.heartbeatFuncMu.Unlock()
}

func (r *RaftTransport) Heartbeat(rpc hraft.RPC) {
	r.heartbeatFuncMu.Lock()
	defer r.heartbeatFuncMu.Unlock()

	if r.heartbeatFunc != nil {
		r.heartbeatFunc(rpc)
	}
}

func NewRaftTransport(localAddress string, trans transport.Transport) *RaftTransport {
	return &RaftTransport{
		rpcChan:      make(chan hraft.RPC),
		localAddress: hraft.ServerAddress(localAddress),
		trans:        trans,
	}
}
