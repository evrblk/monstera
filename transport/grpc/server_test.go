package grpc

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
	"github.com/evrblk/monstera/transport/grpc/monsterapb"
)

// fakeNode is a programmable stand-in for *monstera.Node. Each method delegates
// to an optional hook so a test can inject slow, hanging, or failing behavior;
// unset hooks fall back to a simple echo / zero-value default.
type fakeNode struct {
	raftMessageFn func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error)
	readFn        func(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error)
	updateFn      func(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error)
}

var _ node = (*fakeNode)(nil)

func (f *fakeNode) RaftMessage(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if f.raftMessageFn != nil {
		return f.raftMessageFn(ctx, req)
	}
	// Default: echo the message straight back.
	return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
}

func (f *fakeNode) Read(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error) {
	if f.readFn != nil {
		return f.readFn(ctx, req)
	}
	return &transport.ReadResponse{Payload: req.Payload}, nil
}

func (f *fakeNode) Update(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
	if f.updateFn != nil {
		return f.updateFn(ctx, req)
	}
	return &transport.UpdateResponse{Payload: req.Payload}, nil
}

func (f *fakeNode) TriggerSnapshot(replicaId string) error                          { return nil }
func (f *fakeNode) LeadershipTransfer(replicaId string) error                       { return nil }
func (f *fakeNode) SplitCutoff(ctx context.Context, shardId string) (uint64, error) { return 0, nil }
func (f *fakeNode) ReplicaStates() []*transport.ReplicaState                        { return nil }
func (f *fakeNode) ListSnapshots(replicaId string) ([]raft.SnapshotMetadata, error) {
	return nil, nil
}
func (f *fakeNode) UpdateClusterConfig(ctx context.Context, config *cluster.Config) error { return nil }
func (f *fakeNode) GetClusterConfig() *cluster.Config                                     { return nil }
func (f *fakeNode) Bootstrap(ctx context.Context, nodeId string, config *cluster.Config) error {
	return nil
}

// startTestServer starts a real gRPC server serving the given node over a
// loopback socket and returns its address. The server is stopped on t.Cleanup.
func startTestServer(t *testing.T, n node) (address string, srv *grpc.Server) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	srv = grpc.NewServer(serverOptions()...)
	monsterapb.RegisterMonsteraApiServer(srv, &handler{
		monsteraNode: n,
		logger:       log.New(io.Discard, "", 0),
	})

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return lis.Addr().String(), srv
}

// newTestClient returns a DataPlaneClient pointed at address under nodeId.
func newTestClient(t *testing.T, nodeId, address string) *DataPlaneClient {
	t.Helper()

	c := NewDataPlaneClient()
	c.SetClusterConfig(&cluster.Config{
		Version: 1,
		Nodes:   []*cluster.Node{{Id: nodeId, GrpcAddress: address}},
	})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// startBlackholeListener accepts TCP connections but never speaks HTTP/2, so a
// gRPC client dialing it stays stuck in CONNECTING and stream establishment
// blocks — a deterministic stand-in for an unreachable / black-holed peer that
// needs no real network partition.
func startBlackholeListener(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		for {
			conn, err := lis.Accept()
			if err != nil {
				return
			}
			// Hold the connection open, silent, until teardown.
			go func(c net.Conn) {
				<-done
				_ = c.Close()
			}(conn)
		}
	}()
	t.Cleanup(func() {
		close(done)
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func TestRaftMessageRoundTrip(t *testing.T) {
	addr, _ := startTestServer(t, &fakeNode{})
	c := newTestClient(t, "node-1", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId:   "replica-1",
		MessageType: 7,
		Message:     []byte("ping"),
	})
	if err != nil {
		t.Fatalf("RaftMessage: %v", err)
	}
	if resp.MessageType != 7 || string(resp.Message) != "ping" {
		t.Fatalf("unexpected response: type=%d msg=%q", resp.MessageType, resp.Message)
	}
}

// TestRaftMessageHeadOfLine is the regression test for M14: a slow message must
// not block a later message multiplexed onto the same stream. A "slow" replica
// blocks until released; while it is blocked, a "fast" message must complete.
func TestRaftMessageHeadOfLine(t *testing.T) {
	slowStarted := make(chan struct{})
	release := make(chan struct{})

	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			if req.ReplicaId == "slow" {
				close(slowStarted)
				<-release
			}
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	// Fire the slow message and wait until the server is actually blocked on it.
	slowDone := make(chan error, 1)
	go func() {
		_, err := c.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
			ReplicaId: "slow", MessageType: 1, Message: []byte("slow"),
		})
		slowDone <- err
	}()

	select {
	case <-slowStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("slow handler never started")
	}

	// The fast message shares the stream with the still-blocked slow one. It must
	// complete without waiting for the slow message to be released.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "fast", MessageType: 2, Message: []byte("fast"),
	})
	if err != nil {
		t.Fatalf("fast RaftMessage did not complete while slow was blocked (head-of-line blocking): %v", err)
	}
	if string(resp.Message) != "fast" {
		t.Fatalf("unexpected fast response: %q", resp.Message)
	}

	// Prove the fast message returned strictly before the slow one: the slow call
	// must still be outstanding at this point.
	select {
	case err := <-slowDone:
		t.Fatalf("slow message finished before fast; head-of-line ordering not broken (err=%v)", err)
	default:
	}

	close(release)
	if err := <-slowDone; err != nil {
		t.Fatalf("slow RaftMessage: %v", err)
	}
}

// TestRaftMessageOutOfOrderResponses checks the client correlates responses by
// MessageId even when the server replies in a different order than requests
// arrived.
func TestRaftMessageOutOfOrderResponses(t *testing.T) {
	// Gate each replica's response on its own channel so the test controls the
	// completion order precisely.
	gates := map[string]chan struct{}{
		"a": make(chan struct{}),
		"b": make(chan struct{}),
		"c": make(chan struct{}),
	}
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			<-gates[req.ReplicaId]
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: []byte(req.ReplicaId)}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	type result struct {
		replica string
		msg     string
		err     error
	}
	results := make(chan result, 3)
	for _, r := range []string{"a", "b", "c"} {
		r := r
		go func() {
			resp, err := c.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
				ReplicaId: r, MessageType: 1, Message: []byte(r),
			})
			if err != nil {
				results <- result{replica: r, err: err}
				return
			}
			results <- result{replica: r, msg: string(resp.Message)}
		}()
	}

	// Release in reverse order c, b, a.
	for _, r := range []string{"c", "b", "a"} {
		close(gates[r])
	}

	seen := map[string]string{}
	for i := 0; i < 3; i++ {
		select {
		case res := <-results:
			if res.err != nil {
				t.Fatalf("replica %s: %v", res.replica, res.err)
			}
			seen[res.replica] = res.msg
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for responses")
		}
	}
	for _, r := range []string{"a", "b", "c"} {
		if seen[r] != r {
			t.Fatalf("replica %s got mis-correlated response %q", r, seen[r])
		}
	}
}

// TestRaftMessageHandlerErrorIsolated verifies a per-message handler error no
// longer tears down the whole multiplexed stream: a concurrent good message and
// later messages on the same stream still succeed.
func TestRaftMessageHandlerErrorIsolated(t *testing.T) {
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			if req.ReplicaId == "bad" {
				return nil, fmt.Errorf("boom")
			}
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	// The failing message returns the server's error promptly, via a per-message
	// error envelope — not by timing out and not by killing the stream. Give it a
	// generous deadline so a timeout would clearly indicate a dropped response.
	badCtx, badCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer badCancel()
	_, err := c.RaftMessage(badCtx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "bad", MessageType: 1, Message: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected error for failing message")
	}
	if err == context.DeadlineExceeded {
		t.Fatal("failing message timed out instead of returning an error envelope")
	}
	if got := err.Error(); !strings.Contains(got, "boom") {
		t.Fatalf("error envelope did not carry server error: got %q, want it to contain %q", got, "boom")
	}

	// The stream must still be healthy for other replicas.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "good", MessageType: 2, Message: []byte("ok"),
	})
	if err != nil {
		t.Fatalf("good message after handler error failed (stream torn down?): %v", err)
	}
	if string(resp.Message) != "ok" {
		t.Fatalf("unexpected response: %q", resp.Message)
	}
}

// TestRaftMessageErrorDoesNotDisturbInflight verifies a per-message error
// envelope fails only its own request: a request already in flight on the same
// stream when the error is delivered still completes successfully. This is the
// M16 guarantee — an unrelated request must not fail with the error meant for
// another message.
func TestRaftMessageErrorDoesNotDisturbInflight(t *testing.T) {
	slowStarted := make(chan struct{})
	release := make(chan struct{})
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			switch req.ReplicaId {
			case "slow":
				close(slowStarted)
				<-release
				return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
			case "bad":
				return nil, fmt.Errorf("boom")
			default:
				return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
			}
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	// Put a "slow" request in flight and wait until it is being handled.
	slowDone := make(chan error, 1)
	go func() {
		resp, err := c.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
			ReplicaId: "slow", MessageType: 1, Message: []byte("slow"),
		})
		if err != nil {
			slowDone <- err
			return
		}
		if string(resp.Message) != "slow" {
			slowDone <- fmt.Errorf("unexpected slow response: %q", resp.Message)
			return
		}
		slowDone <- nil
	}()

	select {
	case <-slowStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("slow handler never started")
	}

	// While "slow" is still pending, a "bad" request fails with its own error.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "bad", MessageType: 2, Message: []byte("x"),
	}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected bad message to fail with its own error, got %v", err)
	}

	// The still-pending slow request must not have been collaterally failed.
	select {
	case err := <-slowDone:
		t.Fatalf("in-flight slow request finished early (collateral damage from bad message): %v", err)
	default:
	}

	// Releasing it, it completes cleanly.
	close(release)
	if err := <-slowDone; err != nil {
		t.Fatalf("slow request failed: %v", err)
	}
}

func TestRaftMessageLargePayload(t *testing.T) {
	addr, _ := startTestServer(t, &fakeNode{})
	c := newTestClient(t, "node-1", addr)

	// ~3 MiB, under gRPC's default 4 MiB message limit.
	payload := bytes.Repeat([]byte("abcd"), 3*1024*1024/4)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "r", MessageType: 1, Message: payload,
	})
	if err != nil {
		t.Fatalf("RaftMessage large payload: %v", err)
	}
	if !bytes.Equal(resp.Message, payload) {
		t.Fatalf("large payload not echoed intact: got %d bytes, want %d", len(resp.Message), len(payload))
	}
}

// TestRaftMessageContextTimeout verifies a call whose context expires while the
// server hangs returns the context error, and that the stream stays usable for
// subsequent calls (the timed-out call must clean up after itself).
func TestRaftMessageContextTimeout(t *testing.T) {
	hang := make(chan struct{})
	t.Cleanup(func() { close(hang) })

	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			if req.ReplicaId == "hang" {
				select {
				case <-hang:
				case <-ctx.Done():
				}
				return nil, fmt.Errorf("released")
			}
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: req.Message}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "hang", MessageType: 1, Message: []byte("x"),
	})
	if err == nil {
		t.Fatal("expected context deadline error")
	}

	// Stream must still work for other messages.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	resp, err := c.RaftMessage(ctx2, "node-1", &transport.RaftMessageRequest{
		ReplicaId: "ok", MessageType: 2, Message: []byte("ok"),
	})
	if err != nil {
		t.Fatalf("message after timeout failed: %v", err)
	}
	if string(resp.Message) != "ok" {
		t.Fatalf("unexpected response: %q", resp.Message)
	}
}

// TestRaftMessageServerKill verifies an in-flight call errors out when the
// server is killed mid-request.
func TestRaftMessageServerKill(t *testing.T) {
	stop := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-stop:
		default:
			close(stop)
		}
	})

	started := make(chan struct{})
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			close(started)
			select {
			case <-stop:
			case <-ctx.Done():
			}
			return nil, fmt.Errorf("stopped")
		},
	}
	addr, srv := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	errCh := make(chan error, 1)
	go func() {
		_, err := c.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
			ReplicaId: "r", MessageType: 1, Message: []byte("x"),
		})
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("handler never started")
	}

	srv.Stop()
	close(stop)

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected error after server kill")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight call did not return after server kill")
	}
}

// TestRaftMessageConcurrentRace hammers a single stream with many concurrent
// calls across many replicas. Run with -race to catch data races in the
// server's concurrent dispatch and the client's pending-map correlation.
func TestRaftMessageConcurrentRace(t *testing.T) {
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			// Echo the replica id so callers can verify correlation.
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: []byte(req.ReplicaId)}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	const workers = 32
	const perWorker = 25
	var wg sync.WaitGroup
	var failures atomic.Int64

	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				replica := fmt.Sprintf("r-%d-%d", w, i)
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
					ReplicaId: replica, MessageType: int32(i), Message: []byte(replica),
				})
				cancel()
				if err != nil || string(resp.Message) != replica {
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d concurrent calls returned wrong/failed responses", n)
	}
}

func TestRaftMessageValidation(t *testing.T) {
	addr, _ := startTestServer(t, &fakeNode{})
	c := newTestClient(t, "node-1", addr)

	ctx := context.Background()

	if _, err := c.RaftMessage(ctx, "", &transport.RaftMessageRequest{ReplicaId: "r"}); err == nil {
		t.Fatal("expected error for empty nodeId")
	}
	if _, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{ReplicaId: ""}); err == nil {
		t.Fatal("expected error for empty replicaId")
	}
}

func TestReadUpdateRoundTrip(t *testing.T) {
	n := &fakeNode{
		readFn: func(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error) {
			return &transport.ReadResponse{Payload: append([]byte("read:"), req.Payload...)}, nil
		},
		updateFn: func(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
			return &transport.UpdateResponse{Payload: append([]byte("update:"), req.Payload...)}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rresp, err := c.Read(ctx, "node-1", &transport.ReadRequest{ApplicationName: "app", ShardId: "s1", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(rresp.Payload) != "read:x" {
		t.Fatalf("unexpected read payload: %q", rresp.Payload)
	}

	uresp, err := c.Update(ctx, "node-1", &transport.UpdateRequest{ApplicationName: "app", ShardId: "s1", Payload: []byte("y")})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if string(uresp.Payload) != "update:y" {
		t.Fatalf("unexpected update payload: %q", uresp.Payload)
	}
}

func TestReadErrorPropagation(t *testing.T) {
	n := &fakeNode{
		readFn: func(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error) {
			return nil, fmt.Errorf("read failed")
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.Read(ctx, "node-1", &transport.ReadRequest{ShardId: "s1"}); err == nil {
		t.Fatal("expected read error to propagate")
	}
}

// TestStreamDialDoesNotBlockOtherNodes is the regression test for M15: a
// blocking stream dial to an unreachable node must not stall RaftMessage to a
// healthy node. Before the fix the global streamsMu was held across the dial, so
// the healthy call could not proceed until the black-holed dial resolved (~20s).
func TestStreamDialDoesNotBlockOtherNodes(t *testing.T) {
	healthyAddr, _ := startTestServer(t, &fakeNode{})
	blackAddr := startBlackholeListener(t)

	c := NewDataPlaneClient()
	c.SetClusterConfig(&cluster.Config{
		Version: 1,
		Nodes: []*cluster.Node{
			{Id: "healthy", GrpcAddress: healthyAddr},
			{Id: "black", GrpcAddress: blackAddr},
		},
	})
	t.Cleanup(func() { _ = c.Close() })

	// Fire a call to the black-holed node; it will block establishing its stream.
	blackDone := make(chan struct{})
	go func() {
		defer close(blackDone)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = c.RaftMessage(ctx, "black", &transport.RaftMessageRequest{
			ReplicaId: "r", MessageType: 1, Message: []byte("x"),
		})
	}()

	// Give the black dial a moment to be in flight.
	time.Sleep(200 * time.Millisecond)

	// The healthy node must be reachable while the black dial is stuck.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := c.RaftMessage(ctx, "healthy", &transport.RaftMessageRequest{
		ReplicaId: "r", MessageType: 2, Message: []byte("ok"),
	})
	if err != nil {
		t.Fatalf("healthy node blocked behind black-holed dial: %v", err)
	}
	if string(resp.Message) != "ok" {
		t.Fatalf("unexpected response: %q", resp.Message)
	}

	// The black dial must still be outstanding — proving the healthy call did not
	// wait on it.
	select {
	case <-blackDone:
		t.Fatal("black-holed dial completed unexpectedly fast; test did not exercise the blocking path")
	default:
	}
}

// TestStreamDialRespectsCallerContext verifies a dial to an unreachable node is
// bounded by the caller's context rather than the ~20s gRPC connect timeout (the
// stream's own Background context governs only its post-establishment lifetime).
func TestStreamDialRespectsCallerContext(t *testing.T) {
	blackAddr := startBlackholeListener(t)
	c := newTestClient(t, "black", blackAddr)

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := c.RaftMessage(ctx, "black", &transport.RaftMessageRequest{
		ReplicaId: "r", MessageType: 1, Message: []byte("x"),
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected error dialing black-holed node")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("dial not bounded by caller context: took %v", elapsed)
	}
}

// TestConcurrentStreamCreationSameNode hammers a single node with concurrent
// first-use RaftMessage calls, all of which race to create the one shared
// stream. Run with -race; asserts every call succeeds and is correctly
// correlated.
func TestConcurrentStreamCreationSameNode(t *testing.T) {
	n := &fakeNode{
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			return &transport.RaftMessageResponse{MessageType: req.MessageType, Message: []byte(req.ReplicaId)}, nil
		},
	}
	addr, _ := startTestServer(t, n)
	c := newTestClient(t, "node-1", addr)

	const workers = 48
	var wg sync.WaitGroup
	var failures atomic.Int64
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			replica := fmt.Sprintf("r-%d", w)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
				ReplicaId: replica, MessageType: int32(w), Message: []byte(replica),
			})
			if err != nil || string(resp.Message) != replica {
				failures.Add(1)
			}
		}()
	}
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d concurrent first-use calls failed", n)
	}
}

// startLenientServer is like startTestServer but tolerates very frequent client
// keepalive pings, so tests can use aggressive keepalive for fast black-hole
// detection without being GOAWAY'd.
func startLenientServer(t *testing.T, n node) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	srv := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{
		MinTime:             100 * time.Millisecond,
		PermitWithoutStream: true,
	}))
	monsterapb.RegisterMonsteraApiServer(srv, &handler{
		monsteraNode: n,
		logger:       log.New(io.Discard, "", 0),
	})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String()
}

// startPausableProxy forwards TCP between the caller and backend. While paused it
// stops forwarding in both directions without closing the sockets — a black-hole
// on an already-established connection, which only client keepalive can detect.
func startPausableProxy(t *testing.T, backend string) (addr string, pause, resume func()) {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}

	// A held write-lock blocks the forwarding goroutines (which take the read-lock
	// around each write), suspending both directions.
	var gate sync.RWMutex
	var stateMu sync.Mutex
	paused := false
	pause = func() {
		stateMu.Lock()
		defer stateMu.Unlock()
		if !paused {
			gate.Lock()
			paused = true
		}
	}
	resume = func() {
		stateMu.Lock()
		defer stateMu.Unlock()
		if paused {
			gate.Unlock()
			paused = false
		}
	}

	var connsMu sync.Mutex
	var conns []net.Conn
	forward := func(src, dst net.Conn) {
		buf := make([]byte, 32*1024)
		for {
			n, rerr := src.Read(buf)
			if n > 0 {
				gate.RLock()
				_, werr := dst.Write(buf[:n])
				gate.RUnlock()
				if werr != nil {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}

	go func() {
		for {
			cconn, err := lis.Accept()
			if err != nil {
				return
			}
			bconn, err := net.Dial("tcp", backend)
			if err != nil {
				_ = cconn.Close()
				continue
			}
			connsMu.Lock()
			conns = append(conns, cconn, bconn)
			connsMu.Unlock()
			go forward(cconn, bconn)
			go forward(bconn, cconn)
		}
	}()

	t.Cleanup(func() {
		resume() // unblock any parked forwarders so they can exit
		_ = lis.Close()
		connsMu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		connsMu.Unlock()
	})

	return lis.Addr().String(), pause, resume
}

// TestStreamKeepaliveDetectsBlackhole is the regression test for M17: once an
// established stream's connectivity is silently black-holed, client keepalive
// must tear the stream down (rather than leaving it "alive" in the map forever),
// and a fresh stream must succeed once connectivity is restored.
func TestStreamKeepaliveDetectsBlackhole(t *testing.T) {
	backend := startLenientServer(t, &fakeNode{})
	proxyAddr, pause, resume := startPausableProxy(t, backend)

	c := NewDataPlaneClient(WithClientKeepalive(keepalive.ClientParameters{
		Time:                500 * time.Millisecond,
		Timeout:             500 * time.Millisecond,
		PermitWithoutStream: true,
	}))
	c.SetClusterConfig(&cluster.Config{
		Version: 1,
		Nodes:   []*cluster.Node{{Id: "node-1", GrpcAddress: proxyAddr}},
	})
	t.Cleanup(func() { _ = c.Close() })

	call := func(timeout time.Duration) error {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_, err := c.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{
			ReplicaId: "r", MessageType: 1, Message: []byte("x"),
		})
		return err
	}

	// Establish a healthy stream.
	if err := call(5 * time.Second); err != nil {
		t.Fatalf("initial call failed: %v", err)
	}

	// Black-hole the connection.
	pause()

	// Calls must now fail (keepalive tears the wedged stream down within a couple
	// of ping intervals; before this call each request just burned its deadline).
	deadline := time.Now().Add(10 * time.Second)
	for {
		if err := call(1 * time.Second); err != nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("calls kept succeeding while connection was black-holed")
		}
	}

	// Restore connectivity. A subsequent call must succeed on a fresh stream —
	// the crux of M17: the dead stream must not linger in the map.
	resume()

	deadline = time.Now().Add(15 * time.Second)
	for {
		err := call(2 * time.Second)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream never recovered after connectivity restored: %v", err)
		}
	}
}
