package raft

import (
	"errors"
	"time"

	hraft "github.com/hashicorp/raft"
	"github.com/prometheus/client_golang/prometheus"
)

// Collectors returns all Prometheus collectors defined by this package so the
// parent monstera package can register them alongside its own.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		applyDuration,
		applyErrorsTotal,
		snapshotDuration,
		snapshotBytes,
		snapshotsTotal,
	}
}

var (
	// applyDuration measures a full Raft Apply on the leader: the time from
	// enqueuing a command until it is replicated to a quorum, committed and
	// applied to the FSM. Recorded on both success and failure.
	applyDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_raft_apply_duration_seconds",
		Help:                            "Duration of a Raft Apply (enqueue, replicate, commit, apply) as seen by the leader",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica"})

	// applyErrorsTotal counts failed Raft Apply calls, classified by reason.
	applyErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monstera_raft_apply_errors_total",
		Help: "Number of failed Raft Apply calls by reason",
	}, []string{"application", "shard", "replica", "reason"})

	// Snapshot metrics are shared across two layers: the "install" op is emitted
	// from this package (receiving a snapshot streamed from the leader), while
	// the "persist" and "restore" ops are emitted from the monstera package at
	// the appCoreAdapter boundary. All emission goes through RecordSnapshot so the
	// vecs stay defined in a single place. op is one of "persist", "restore",
	// "install".
	snapshotDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_raft_snapshot_duration_seconds",
		Help:                            "Duration of Raft snapshot operations by op (persist, restore, install)",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica", "op"})

	snapshotBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_raft_snapshot_bytes",
		Help:                            "Size in bytes of Raft snapshots by op (persist, restore, install)",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica", "op"})

	snapshotsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monstera_raft_snapshots_total",
		Help: "Number of Raft snapshot operations by op and result",
	}, []string{"application", "shard", "replica", "op", "result"})
)

// RecordSnapshot records metrics for a single Raft snapshot operation. op is one
// of "persist", "restore" or "install". On error only the error count is
// recorded; on success the duration and byte size are observed too. It is the
// single emission point for the snapshot metric family, called both from this
// package (install) and from the monstera package (persist, restore).
func RecordSnapshot(applicationName string, shardId string, replicaId string, op string, duration time.Duration, bytes int64, err error) {
	if err != nil {
		snapshotsTotal.WithLabelValues(applicationName, shardId, replicaId, op, "error").Inc()
		return
	}
	snapshotDuration.WithLabelValues(applicationName, shardId, replicaId, op).Observe(duration.Seconds())
	snapshotBytes.WithLabelValues(applicationName, shardId, replicaId, op).Observe(float64(bytes))
	snapshotsTotal.WithLabelValues(applicationName, shardId, replicaId, op, "ok").Inc()
}

// applyErrorReason classifies an error returned by hraft.Apply into a small,
// bounded set of reasons suitable for a metric label.
func applyErrorReason(err error) string {
	switch {
	case errors.Is(err, hraft.ErrEnqueueTimeout):
		return "timeout"
	case errors.Is(err, hraft.ErrLeadershipLost), errors.Is(err, hraft.ErrLeadershipTransferInProgress):
		return "leadership_lost"
	case errors.Is(err, hraft.ErrNotLeader):
		return "not_leader"
	case errors.Is(err, hraft.ErrRaftShutdown):
		return "shutdown"
	case errors.Is(err, hraft.ErrAbortedByRestore):
		return "aborted_by_restore"
	default:
		return "other"
	}
}
