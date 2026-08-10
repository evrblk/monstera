package raft

import (
	"bytes"
	"io"
	"testing"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"

	"github.com/evrblk/monstera/internal/raft/raftpb"
	"github.com/evrblk/monstera/transport"
)

// snapshotCopy records the outcome of the fake main loop's io.Copy of one
// InstallSnapshot pipe.
type snapshotCopy struct {
	data []byte
	err  error
}

// newSnapshotTestRaft builds a Raft with only the transport wired (no hraft) and
// a goroutine that drains the RPC channel the way hraft's main goroutine does:
// io.Copy-ing each InstallSnapshot reader (which blocks until the pipe is
// closed) and then responding. This reproduces the real "main goroutine blocked
// on the snapshot pipe" condition — the thing C2 could wedge forever — without a
// full cluster.
func newSnapshotTestRaft(t *testing.T, timeout time.Duration) (*Raft, <-chan snapshotCopy) {
	t.Helper()

	r := &Raft{
		transport:              NewRaftTransport("node_1", &nopTransport{}),
		nodeId:                 "node_1",
		applicationName:        "Core",
		shardId:                "s1",
		replicaId:              "r1",
		snapshotSessionTimeout: timeout,
	}

	copies := make(chan snapshotCopy, 16)
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			case rpc := <-r.transport.Consumer():
				var buf bytes.Buffer
				_, err := io.Copy(&buf, rpc.Reader)
				rpc.Respond(&hraft.InstallSnapshotResponse{Success: err == nil}, err)
				copies <- snapshotCopy{data: buf.Bytes(), err: err}
			}
		}
	}()
	t.Cleanup(func() {
		// Free the main loop if it is still blocked on a pipe, then stop it.
		if s := currentSnapshotSession(r); s != nil {
			r.abandonSnapshotSession(s, errSnapshotSessionTimeout)
		}
		close(stop)
	})

	return r, copies
}

func currentSnapshotSession(r *Raft) *snapshotSession {
	r.snapshotSessionMu.Lock()
	defer r.snapshotSessionMu.Unlock()
	return r.snapshotSession
}

func initSnapshotSession(t *testing.T, r *Raft, size int64) uint64 {
	t.Helper()
	msg, err := (&raftpb.InstallSnapshotInitRequest{RpcHeader: &raftpb.RPCHeader{}, Term: 1, Size: size}).MarshalVT()
	require.NoError(t, err)

	resp, err := r.RaftMessage(&transport.RaftMessageRequest{
		MessageType: InstallSnapshotInitRequest,
		Message:     msg,
	})
	require.NoError(t, err)
	require.Equal(t, InstallSnapshotInitResponse, resp.MessageType)

	initResp := &raftpb.InstallSnapshotInitResponse{}
	require.NoError(t, initResp.UnmarshalVT(resp.Message))
	return initResp.SessionId
}

func sendSnapshotChunk(t *testing.T, r *Raft, sessionID uint64, data []byte) (*transport.RaftMessageResponse, error) {
	t.Helper()
	msg, err := (&raftpb.InstallSnapshotChunkRequest{Data: data, SessionId: sessionID}).MarshalVT()
	require.NoError(t, err)
	return r.RaftMessage(&transport.RaftMessageRequest{
		MessageType: InstallSnapshotChunkRequest,
		Message:     msg,
	})
}

// TestInstallSnapshotSessionTimeout is the regression test for C2: when the
// sender vanishes after Init (no further chunks), the inactivity deadline must
// abandon the session and free the main Raft goroutine blocked on the pipe.
func TestInstallSnapshotSessionTimeout(t *testing.T) {
	r, copies := newSnapshotTestRaft(t, 150*time.Millisecond)

	id := initSnapshotSession(t, r, 1000) // promises 1000 bytes that never arrive
	require.NotZero(t, id)

	select {
	case cp := <-copies:
		require.Error(t, cp.err, "io.Copy should end with the abandonment error")
	case <-time.After(3 * time.Second):
		t.Fatal("main loop never unblocked; the abandoned session wedged it (C2)")
	}

	require.Nil(t, currentSnapshotSession(r), "abandoned session must be cleared")
}

// TestInstallSnapshotDeadlineExtendedByProgress verifies the deadline is an
// inactivity timeout: a steady chunk stream keeps a transfer alive well past a
// single timeout window and completes.
func TestInstallSnapshotDeadlineExtendedByProgress(t *testing.T) {
	r, copies := newSnapshotTestRaft(t, 150*time.Millisecond)

	payload := bytes.Repeat([]byte("z"), 500)
	id := initSnapshotSession(t, r, int64(len(payload)))

	// Feed chunks slower than one timeout window each, but never idle long enough
	// to trip it, for longer than a single window in total.
	for i := 0; i < len(payload); i += 50 {
		time.Sleep(50 * time.Millisecond)
		end := i + 50
		resp, err := sendSnapshotChunk(t, r, id, payload[i:end])
		require.NoError(t, err)
		require.Equal(t, InstallSnapshotChunkResponse, resp.MessageType)
	}

	select {
	case cp := <-copies:
		require.NoError(t, cp.err)
		require.Equal(t, payload, cp.data)
	case <-time.After(3 * time.Second):
		t.Fatal("steady transfer did not complete")
	}
	require.Nil(t, currentSnapshotSession(r))
}

// TestInstallSnapshotRejectsStaleSessionId verifies a chunk stamped with a
// different session id (a stray chunk from a superseded stream) is rejected and
// does not disturb the active session.
func TestInstallSnapshotRejectsStaleSessionId(t *testing.T) {
	r, _ := newSnapshotTestRaft(t, 10*time.Second)

	id := initSnapshotSession(t, r, 1000)

	_, err := sendSnapshotChunk(t, r, id+1, []byte("stray"))
	require.Error(t, err, "a chunk with a mismatched session id must be rejected")

	s := currentSnapshotSession(r)
	require.NotNil(t, s, "active session must survive a stray chunk")
	require.Equal(t, id, s.id)
	require.Zero(t, s.bytesReceived, "stray chunk must not have been written")
}

// TestInstallSnapshotSupersede verifies a new Init tears down the previous
// session, freeing its blocked main-loop io.Copy, and installs a fresh session.
func TestInstallSnapshotSupersede(t *testing.T) {
	r, copies := newSnapshotTestRaft(t, 10*time.Second)

	idA := initSnapshotSession(t, r, 1000)
	// Push one chunk so the main loop is definitely io.Copy-ing session A: the
	// write returns only once the reader has consumed it. Without this the two
	// install goroutines would race on the unbuffered RPC channel and the main
	// loop might pick up B first, so A would never start copying.
	resp, err := sendSnapshotChunk(t, r, idA, []byte("first"))
	require.NoError(t, err)
	require.Equal(t, InstallSnapshotChunkResponse, resp.MessageType)

	idB := initSnapshotSession(t, r, 1000)
	require.NotEqual(t, idA, idB, "each session gets a distinct id")

	select {
	case cp := <-copies:
		require.Error(t, cp.err, "superseded session's io.Copy should be freed with an error")
	case <-time.After(3 * time.Second):
		t.Fatal("superseded session's main-loop copy was not freed")
	}

	s := currentSnapshotSession(r)
	require.NotNil(t, s)
	require.Equal(t, idB, s.id, "the newest session must be the active one")
}

// TestInstallSnapshotHappyPath verifies a normal multi-chunk transfer: chunks
// carry the returned session id, the bytes arrive intact, and the session
// clears on completion.
func TestInstallSnapshotHappyPath(t *testing.T) {
	r, copies := newSnapshotTestRaft(t, 10*time.Second)

	payload := bytes.Repeat([]byte("abcd"), 4096) // 16 KiB
	id := initSnapshotSession(t, r, int64(len(payload)))

	half := len(payload) / 2
	resp, err := sendSnapshotChunk(t, r, id, payload[:half])
	require.NoError(t, err)
	require.Equal(t, InstallSnapshotChunkResponse, resp.MessageType)

	resp, err = sendSnapshotChunk(t, r, id, payload[half:])
	require.NoError(t, err)
	require.Equal(t, InstallSnapshotChunkResponse, resp.MessageType)

	select {
	case cp := <-copies:
		require.NoError(t, cp.err)
		require.Equal(t, payload, cp.data)
	case <-time.After(3 * time.Second):
		t.Fatal("snapshot transfer never completed")
	}
	require.Nil(t, currentSnapshotSession(r))
}
