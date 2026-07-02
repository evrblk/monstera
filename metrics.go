package monstera

import (
	"io"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/evrblk/monstera/internal/raft"
)

var (
	// fsmApplyDuration measures applying a single committed log entry to the
	// application core (command decode plus the core Update). Recorded on every
	// replica that applies the entry, not just the leader. Emitted by
	// appCoreAdapter.Apply, which the Raft FSM delegates to.
	fsmApplyDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_raft_fsm_apply_duration_seconds",
		Help:                            "Duration of applying a committed log entry to the application core (decode and core update)",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica"})

	// commitsTotal counts committed log entries applied to the application core.
	// Incremented on every replica, so it reflects local apply throughput.
	commitsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monstera_raft_commits_total",
		Help: "Number of committed log entries applied to the application core",
	}, []string{"application", "shard", "replica"})

	// replicaUpdateDuration measures a replica Update end to end: encoding the
	// command and running it through Raft (replicate, commit, apply). Recorded
	// on both success and failure.
	replicaUpdateDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_replica_update_duration_seconds",
		Help:                            "Duration of a replica Update (command encode plus Raft apply)",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica"})

	// replicaReadDuration measures a replica Read served locally by the core.
	// Recorded on both success and failure (including recovered panics).
	replicaReadDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_replica_read_duration_seconds",
		Help:                            "Duration of a replica Read served by the core",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica"})

	// replicaUpdatesTotal counts replica Updates by result (ok/error).
	replicaUpdatesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monstera_replica_updates_total",
		Help: "Number of replica Updates by result",
	}, []string{"application", "shard", "replica", "result"})

	// replicaReadsTotal counts replica Reads by result (ok/error).
	replicaReadsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "monstera_replica_reads_total",
		Help: "Number of replica Reads by result",
	}, []string{"application", "shard", "replica", "result"})

	// replicaCommandBytes measures the size in bytes of the encoded command
	// handed to Raft on each Update.
	replicaCommandBytes = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:                            "monstera_replica_command_bytes",
		Help:                            "Size in bytes of the encoded command submitted to Raft on Update",
		NativeHistogramBucketFactor:     1.1,
		NativeHistogramMaxBucketNumber:  100,
		NativeHistogramMinResetDuration: time.Hour,
	}, []string{"application", "shard", "replica"})
)

// RegisterMetrics registers all Prometheus metrics emitted by the Monstera
// framework (node, replica and Raft layers) with the given registerer. Call it
// once at startup, e.g. monstera.RegisterMetrics(prometheus.DefaultRegisterer),
// before serving. It panics if a metric is already registered.
//
// Note: per-core RPC metrics live in the generated adapters, not here.
func RegisterMetrics(registerer prometheus.Registerer) {
	collectors := []prometheus.Collector{
		fsmApplyDuration,
		commitsTotal,
		replicaUpdateDuration,
		replicaReadDuration,
		replicaUpdatesTotal,
		replicaReadsTotal,
		replicaCommandBytes,
	}
	collectors = append(collectors, raft.Collectors()...)

	for _, c := range collectors {
		registerer.MustRegister(c)
	}
}

// countingWriter wraps an io.Writer and counts the bytes written through it.
type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// countingReadCloser wraps an io.ReadCloser and counts the bytes read from it.
type countingReadCloser struct {
	r io.ReadCloser
	n int64
}

func (c *countingReadCloser) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

func (c *countingReadCloser) Close() error {
	return c.r.Close()
}
