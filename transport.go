package monstera

import (
	"context"
	"io"
	"sync"
	"time"

	"google.golang.org/grpc"

	hraft "github.com/hashicorp/raft"
)

// These are calls from the Raft engine that we need to send out over gRPC.
type RaftGrpcTransport struct {
	rpcChan         chan hraft.RPC
	localAddress    hraft.ServerAddress
	heartbeatFunc   func(hraft.RPC)
	heartbeatFuncMu sync.Mutex

	pool *MonsteraConnectionPool
}

var _ hraft.Transport = &RaftGrpcTransport{}

func (r *RaftGrpcTransport) Producer() chan hraft.RPC {
	return r.rpcChan
}

// Consumer returns a channel that can be used to consume and respond to RPC requests.
func (r *RaftGrpcTransport) Consumer() <-chan hraft.RPC {
	return r.rpcChan
}

// LocalAddr is used to return our local address to distinguish from our peers.
func (r *RaftGrpcTransport) LocalAddr() hraft.ServerAddress {
	return r.localAddress
}

// AppendEntries sends the appropriate RPC to the target node.
func (r *RaftGrpcTransport) AppendEntries(id hraft.ServerID, target hraft.ServerAddress, args *hraft.AppendEntriesRequest, resp *hraft.AppendEntriesResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := r.pool.GetConnection(string(target))
	if err != nil {
		return err
	}
	ret, err := conn.AppendEntries(ctx, encodeAppendEntriesRequest(args, string(id)), grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	*resp = *decodeAppendEntriesResponse(ret)
	return nil
}

// RequestVote sends the appropriate RPC to the target node.
func (r *RaftGrpcTransport) RequestVote(id hraft.ServerID, target hraft.ServerAddress, args *hraft.RequestVoteRequest, resp *hraft.RequestVoteResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := r.pool.GetConnection(string(target))
	if err != nil {
		return err
	}
	ret, err := conn.RequestVote(ctx, encodeRequestVoteRequest(args, string(id)), grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	*resp = *decodeRequestVoteResponse(ret)
	return nil
}

// TimeoutNow is used to start a leadership transfer to the target node.
func (r *RaftGrpcTransport) TimeoutNow(id hraft.ServerID, target hraft.ServerAddress, args *hraft.TimeoutNowRequest, resp *hraft.TimeoutNowResponse) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := r.pool.GetConnection(string(target))
	if err != nil {
		return err
	}
	ret, err := conn.TimeoutNow(ctx, encodeTimeoutNowRequest(args, string(id)), grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	*resp = *decodeTimeoutNowResponse(ret)
	return nil
}

// InstallSnapshot is used to push a snapshot down to a follower. The data is read from
// the ReadCloser and streamed to the client.
func (r *RaftGrpcTransport) InstallSnapshot(id hraft.ServerID, target hraft.ServerAddress, req *hraft.InstallSnapshotRequest, resp *hraft.InstallSnapshotResponse, data io.Reader) error {
	conn, err := r.pool.GetConnection(string(target))
	if err != nil {
		return err
	}
	stream, err := conn.InstallSnapshot(context.TODO(), grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	if err := stream.Send(encodeInstallSnapshotRequest(req, string(id))); err != nil {
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
		if err := stream.Send(&InstallSnapshotRequest{
			Data: buf[:n],
		}); err != nil {
			return err
		}
	}
	ret, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	*resp = *decodeInstallSnapshotResponse(ret)
	return nil
}

// AppendEntriesPipeline returns an interface that can be used to pipeline
// AppendEntries requests.
func (r *RaftGrpcTransport) AppendEntriesPipeline(id hraft.ServerID, target hraft.ServerAddress) (hraft.AppendPipeline, error) {
	conn, err := r.pool.GetConnection(string(target))
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	ctx, cancel := context.WithCancel(ctx)
	stream, err := conn.AppendEntriesPipeline(ctx, grpc.WaitForReady(true))
	if err != nil {
		cancel()
		return nil, err
	}
	rpa := &raftPipelineAPI{
		stream:          stream,
		cancel:          cancel,
		inflightCh:      make(chan *appendFuture, 20),
		doneCh:          make(chan hraft.AppendFuture, 20),
		targetReplicaId: string(id),
	}
	go rpa.receiver()
	return rpa, nil
}

type raftPipelineAPI struct {
	stream          MonsteraApi_AppendEntriesPipelineClient
	cancel          func()
	inflightChMtx   sync.Mutex
	inflightCh      chan *appendFuture
	doneCh          chan hraft.AppendFuture
	targetReplicaId string
}

// AppendEntries is used to add another request to the pipeline.
// The send may block which is an effective form of back-pressure.
func (r *raftPipelineAPI) AppendEntries(req *hraft.AppendEntriesRequest, resp *hraft.AppendEntriesResponse) (hraft.AppendFuture, error) {
	af := &appendFuture{
		start:   time.Now(),
		request: req,
		done:    make(chan struct{}),
	}
	if err := r.stream.Send(encodeAppendEntriesRequest(req, r.targetReplicaId)); err != nil {
		return nil, err
	}
	r.inflightChMtx.Lock()
	select {
	case <-r.stream.Context().Done():
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
		msg, err := r.stream.Recv()
		if err != nil {
			af.err = err
		} else {
			af.response = *decodeAppendEntriesResponse(msg)
		}
		close(af.done)
		r.doneCh <- af
	}
}

type appendFuture struct {
	hraft.AppendFuture

	start    time.Time
	request  *hraft.AppendEntriesRequest
	response hraft.AppendEntriesResponse
	err      error
	done     chan struct{}
}

// Error blocks until the future arrives and then
// returns the error status of the future.
// This may be called any number of times - all
// calls will return the same value.
// Note that it is not OK to call this method
// twice concurrently on the same Future instance.
func (f *appendFuture) Error() error {
	<-f.done
	return f.err
}

// Start returns the time that the append request was started.
// It is always OK to call this method.
func (f *appendFuture) Start() time.Time {
	return f.start
}

// Request holds the parameters of the AppendEntries call.
// It is always OK to call this method.
func (f *appendFuture) Request() *hraft.AppendEntriesRequest {
	return f.request
}

// Response holds the results of the AppendEntries call.
// This method must only be called after the Error
// method returns, and will only be valid on success.
func (f *appendFuture) Response() *hraft.AppendEntriesResponse {
	return &f.response
}

// EncodePeer is used to serialize a peer's address.
func (r *RaftGrpcTransport) EncodePeer(id hraft.ServerID, addr hraft.ServerAddress) []byte {
	return []byte(addr)
}

// DecodePeer is used to deserialize a peer's address.
func (r *RaftGrpcTransport) DecodePeer(p []byte) hraft.ServerAddress {
	return hraft.ServerAddress(p)
}

// SetHeartbeatHandler is used to setup a heartbeat handler
// as a fast-pass. This is to avoid head-of-line blocking from
// disk IO. If a Transport does not support this, it can simply
// ignore the call, and push the heartbeat onto the Consumer channel.
func (r *RaftGrpcTransport) SetHeartbeatHandler(cb func(rpc hraft.RPC)) {
	r.heartbeatFuncMu.Lock()
	r.heartbeatFunc = cb
	r.heartbeatFuncMu.Unlock()
}

func (r *RaftGrpcTransport) Heartbeat(rpc hraft.RPC) {
	r.heartbeatFuncMu.Lock()
	defer r.heartbeatFuncMu.Unlock()

	if r.heartbeatFunc != nil {
		r.heartbeatFunc(rpc)
	}
}

func NewTransport(localAddress string, pool *MonsteraConnectionPool) *RaftGrpcTransport {
	return &RaftGrpcTransport{
		rpcChan:      make(chan hraft.RPC),
		localAddress: hraft.ServerAddress(localAddress),
		pool:         pool,
	}
}
