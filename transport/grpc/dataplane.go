package grpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// DefaultClientKeepalive probes each pooled connection so a black-holed peer is
// detected even when no application traffic flows: without it a persistent Raft
// stream to a silently-dropped peer is only ever noticed via a Recv error, which
// never arrives, so every send burns its full deadline and the dead stream is
// never replaced — even after connectivity is restored. Time+Timeout (~13s)
// stays well under the Raft transport's 30s per-call deadline. PermitWithoutStream
// keeps probing during idle gaps between streams. Must stay >= the server's
// EnforcementPolicy.MinTime (see defaultServerKeepaliveEnforcement) or the server
// will GOAWAY the connection for pinging too aggressively.
var DefaultClientKeepalive = keepalive.ClientParameters{
	Time:                10 * time.Second,
	Timeout:             3 * time.Second,
	PermitWithoutStream: true,
}

type dataPlaneOptions struct {
	keepalive keepalive.ClientParameters
}

// DataPlaneOption customizes a DataPlaneClient.
type DataPlaneOption func(*dataPlaneOptions)

// WithClientKeepalive overrides the gRPC client keepalive parameters (mainly for
// tests that need faster black-hole detection).
func WithClientKeepalive(kp keepalive.ClientParameters) DataPlaneOption {
	return func(o *dataPlaneOptions) { o.keepalive = kp }
}

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

	// streamsMu guards only the streams map itself — inserting/looking up/removing
	// per-node entries. It is never held across a stream dial (which can block for
	// seconds on an unreachable node); dialing is serialized per node by the
	// entry's own creation lock instead, so a black-holed peer cannot stall Raft
	// traffic to healthy nodes. See streamEntry.
	streamsMu sync.Mutex
	streams   map[string]*streamEntry
}

// streamEntry owns the single multiplexed Raft stream to one node. Its creation
// lock (a capacity-1 channel, so it can be acquired subject to the caller's
// context) serializes dialing per node without touching the global streamsMu.
type streamEntry struct {
	// lock is a capacity-1 semaphore held by whichever goroutine is creating the
	// stream. Acquired via select so a caller waiting for an in-flight dial can
	// still honor its own deadline.
	lock chan struct{}

	// mu guards stream and removed. It is only ever held for map-like O(1) work,
	// never across a dial.
	mu      sync.Mutex
	stream  *raftMessageStream
	removed bool // set once the node leaves the config or the client closes
}

func newStreamEntry() *streamEntry {
	return &streamEntry{lock: make(chan struct{}, 1)}
}

// live returns the current stream if it exists and is not dead, clearing it
// otherwise so the next caller redials.
func (e *streamEntry) live() *raftMessageStream {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.stream == nil {
		return nil
	}
	select {
	case <-e.stream.dead:
		e.stream = nil
		return nil
	default:
		return e.stream
	}
}

// store publishes a freshly dialed stream. It returns false (and cancels s) if
// the entry was removed while the dial was in flight, so the stream is never
// orphaned.
func (e *streamEntry) store(s *raftMessageStream) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.removed {
		s.cancel()
		return false
	}
	e.stream = s
	return true
}

// close marks the entry removed and tears down any live stream. A dial that is
// still in flight will see removed and cancel its result in store().
func (e *streamEntry) close() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removed = true
	if e.stream != nil {
		e.stream.cancel()
		e.stream = nil
	}
}

var _ transport.DataPlane = &DataPlaneClient{}
var _ transport.ClusterConfigConsumer = &DataPlaneClient{}

func NewDataPlaneClient(opts ...DataPlaneOption) *DataPlaneClient {
	cfg := dataPlaneOptions{keepalive: DefaultClientKeepalive}
	for _, o := range opts {
		o(&cfg)
	}

	return &DataPlaneClient{
		pool: NewGrpcClientPool[monsterapb.MonsteraApiClient](func(conn *grpc.ClientConn) monsterapb.MonsteraApiClient {
			return monsterapb.NewMonsteraApiClient(conn)
		}, grpc.WithKeepaliveParams(cfg.keepalive)),
		streams: make(map[string]*streamEntry),
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
	var removed []*streamEntry
	for id, e := range t.streams {
		if !newIds[id] {
			removed = append(removed, e)
			delete(t.streams, id)
		}
	}
	t.streamsMu.Unlock()

	// Tear the removed entries down outside streamsMu.
	for _, e := range removed {
		e.close()
	}
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
		ShardKey:               encodeShardKey(req.ShardKey, req.HasShardKey),
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
		ShardKey:        encodeShardKey(req.ShardKey, req.HasShardKey),
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

	s, err := t.getOrCreateStream(ctx, nodeId)
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
	entries := make([]*streamEntry, 0, len(t.streams))
	for _, e := range t.streams {
		entries = append(entries, e)
	}
	t.streams = make(map[string]*streamEntry)
	t.streamsMu.Unlock()

	for _, e := range entries {
		e.close()
	}

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

func (t *DataPlaneClient) getOrCreateStream(ctx context.Context, nodeId string) (*raftMessageStream, error) {
	// Look up (or create) the per-node entry under the global lock. This is O(1)
	// and never blocks on I/O.
	t.streamsMu.Lock()
	e, ok := t.streams[nodeId]
	if !ok {
		e = newStreamEntry()
		t.streams[nodeId] = e
	}
	t.streamsMu.Unlock()

	// Fast path: a live stream already exists.
	if s := e.live(); s != nil {
		return s, nil
	}

	// Acquire the per-node creation lock, subject to the caller's context so a
	// slow or black-holed dial to this node can never block the caller past its
	// own deadline — and, crucially, never blocks callers for other nodes.
	select {
	case e.lock <- struct{}{}:
		defer func() { <-e.lock }()
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	// Re-check: another goroutine may have created the stream while we waited.
	if s := e.live(); s != nil {
		return s, nil
	}

	conn, err := t.getConnection(nodeId)
	if err != nil {
		return nil, err
	}

	// Dial outside streamsMu; bound establishment by the caller's context.
	s, err := newRaftMessageStream(ctx, conn)
	if err != nil {
		return nil, err
	}
	if !e.store(s) {
		// The node left the config (or the client closed) while we were dialing;
		// store() already cancelled the stream.
		return nil, fmt.Errorf("stream to node %s was removed", nodeId)
	}
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
	// closed is set by recvLoop once the stream has died. send checks it under
	// pendingMu before registering into pending, so a send that races stream
	// death returns deadErr instead of writing to a torn-down map.
	closed bool

	dead    chan struct{}
	deadErr error
}

// newRaftMessageStream opens a persistent multiplexed Raft stream. The stream
// outlives any single RaftMessage call, so its lifetime is governed by a
// Background-derived context (cancelled via the returned stream's cancel), not
// by dialCtx. dialCtx bounds only establishment: creating a client stream to an
// unreachable peer can block for seconds while gRPC connects, and the caller
// must not be pinned to that beyond its own deadline.
func newRaftMessageStream(dialCtx context.Context, conn monsterapb.MonsteraApiClient) (*raftMessageStream, error) {
	streamCtx, cancel := context.WithCancel(context.Background())

	type result struct {
		stream grpc.BidiStreamingClient[monsterapb.RaftMessageRequest, monsterapb.RaftMessageResponse]
		err    error
	}
	// Buffered so the goroutine never leaks if we abandon on dialCtx.Done.
	ch := make(chan result, 1)
	go func() {
		stream, err := conn.RaftMessage(streamCtx)
		ch <- result{stream: stream, err: err}
	}()

	select {
	case r := <-ch:
		if r.err != nil {
			cancel()
			return nil, r.err
		}
		s := &raftMessageStream{
			stream:  r.stream,
			cancel:  cancel,
			pending: make(map[int64]chan *monsterapb.RaftMessageResponse),
			dead:    make(chan struct{}),
		}
		go s.recvLoop()
		return s, nil
	case <-dialCtx.Done():
		// Abandon the establishment: cancelling streamCtx unblocks the goroutine's
		// conn.RaftMessage, which then discards its result into the buffered channel.
		cancel()
		return nil, dialCtx.Err()
	}
}

func (s *raftMessageStream) recvLoop() {
	for {
		resp, err := s.stream.Recv()
		if err != nil {
			s.deadErr = err
			close(s.dead)
			s.pendingMu.Lock()
			s.closed = true
			for id, ch := range s.pending {
				close(ch)
				delete(s.pending, id)
			}
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
	if s.closed {
		s.pendingMu.Unlock()
		return nil, s.deadErr
	}
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
		// A non-empty Error is a per-message failure envelope: the server could
		// not handle this specific message but the shared stream is healthy.
		if resp.Error != "" {
			return nil, errors.New(resp.Error)
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
