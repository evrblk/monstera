package local

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/evrblk/monstera/cluster"
	"github.com/evrblk/monstera/internal/raft"
	"github.com/evrblk/monstera/transport"
)

// fakeNode is a programmable stand-in for *monstera.Node used to drive the local
// transport with slow, hanging, or failing behavior.
type fakeNode struct {
	id string

	raftMessageFn func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error)
	readFn        func(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error)
	updateFn      func(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error)
}

var _ localNode = (*fakeNode)(nil)

func (f *fakeNode) NodeId() string { return f.id }

func (f *fakeNode) RaftMessage(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
	if f.raftMessageFn != nil {
		return f.raftMessageFn(ctx, req)
	}
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

// register inserts a fake node directly into the registry (bypassing the
// *monstera.Node-typed public Register), which is why these tests live in
// package local.
func register(t *LocalTransport, n localNode) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[n.NodeId()] = n
}

func TestLocalRaftMessageRoundTrip(t *testing.T) {
	tr := NewLocalTransport()
	register(tr, &fakeNode{id: "node-1"})

	resp, err := tr.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
		ReplicaId: "r", MessageType: 9, Message: []byte("hi"),
	})
	if err != nil {
		t.Fatalf("RaftMessage: %v", err)
	}
	if resp.MessageType != 9 || string(resp.Message) != "hi" {
		t.Fatalf("unexpected response: type=%d msg=%q", resp.MessageType, resp.Message)
	}
}

func TestLocalRaftMessageValidation(t *testing.T) {
	tr := NewLocalTransport()
	register(tr, &fakeNode{id: "node-1"})

	ctx := context.Background()
	if _, err := tr.RaftMessage(ctx, "", &transport.RaftMessageRequest{ReplicaId: "r"}); err == nil {
		t.Fatal("expected error for empty nodeId")
	}
	if _, err := tr.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{ReplicaId: ""}); err == nil {
		t.Fatal("expected error for empty replicaId")
	}
}

func TestLocalRaftMessageUnknownNode(t *testing.T) {
	tr := NewLocalTransport()
	if _, err := tr.RaftMessage(context.Background(), "missing", &transport.RaftMessageRequest{ReplicaId: "r"}); err == nil {
		t.Fatal("expected error for unknown node")
	}
}

// TestLocalContextPropagation verifies the transport passes the caller's context
// straight through to the node, so a cancelled context is observed downstream.
func TestLocalContextPropagation(t *testing.T) {
	tr := NewLocalTransport()
	register(tr, &fakeNode{
		id: "node-1",
		raftMessageFn: func(ctx context.Context, req *transport.RaftMessageRequest) (*transport.RaftMessageResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := tr.RaftMessage(ctx, "node-1", &transport.RaftMessageRequest{ReplicaId: "r"})
	if err == nil {
		t.Fatal("expected context error")
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("context cancellation not propagated promptly (took %v)", elapsed)
	}
}

func TestLocalLargePayload(t *testing.T) {
	tr := NewLocalTransport()
	register(tr, &fakeNode{id: "node-1"})

	payload := bytes.Repeat([]byte("z"), 8*1024*1024) // 8 MiB; no wire limit in-memory.
	resp, err := tr.RaftMessage(context.Background(), "node-1", &transport.RaftMessageRequest{
		ReplicaId: "r", MessageType: 1, Message: payload,
	})
	if err != nil {
		t.Fatalf("RaftMessage: %v", err)
	}
	if !bytes.Equal(resp.Message, payload) {
		t.Fatalf("payload not passed through intact: got %d bytes", len(resp.Message))
	}
}

func TestLocalReadUpdatePassThrough(t *testing.T) {
	tr := NewLocalTransport()
	register(tr, &fakeNode{
		id: "node-1",
		readFn: func(ctx context.Context, req *transport.ReadRequest) (*transport.ReadResponse, error) {
			return &transport.ReadResponse{Payload: append([]byte("r:"), req.Payload...)}, nil
		},
		updateFn: func(ctx context.Context, req *transport.UpdateRequest) (*transport.UpdateResponse, error) {
			return nil, fmt.Errorf("update failed")
		},
	})

	rresp, err := tr.Read(context.Background(), "node-1", &transport.ReadRequest{ShardId: "s", Payload: []byte("x")})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if string(rresp.Payload) != "r:x" {
		t.Fatalf("unexpected read payload: %q", rresp.Payload)
	}

	if _, err := tr.Update(context.Background(), "node-1", &transport.UpdateRequest{ShardId: "s"}); err == nil {
		t.Fatal("expected update error to propagate")
	}
}

// TestLocalConcurrentRegistryRace exercises the RWMutex-guarded registry: many
// goroutines dispatch calls while more goroutines register/re-register nodes.
// Run with -race.
func TestLocalConcurrentRegistryRace(t *testing.T) {
	tr := NewLocalTransport()
	const nodes = 16
	for i := 0; i < nodes; i++ {
		register(tr, &fakeNode{id: fmt.Sprintf("node-%d", i)})
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: keep re-registering nodes.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for j := 0; j < nodes; j++ {
					register(tr, &fakeNode{id: fmt.Sprintf("node-%d", j)})
				}
			}
		}()
	}

	// Readers: dispatch RaftMessage calls concurrently.
	var failures int64
	var fmu sync.Mutex
	for i := 0; i < 16; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 200; k++ {
				id := fmt.Sprintf("node-%d", (i+k)%nodes)
				resp, err := tr.RaftMessage(context.Background(), id, &transport.RaftMessageRequest{
					ReplicaId: "r", MessageType: int32(k), Message: []byte(id),
				})
				if err != nil || string(resp.Message) != id {
					fmu.Lock()
					failures++
					fmu.Unlock()
				}
			}
		}()
	}

	// Let readers run, then stop writers.
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	if failures != 0 {
		t.Fatalf("%d calls failed during concurrent registry access", failures)
	}
}

func TestLocalClose(t *testing.T) {
	tr := NewLocalTransport()
	if err := tr.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}
