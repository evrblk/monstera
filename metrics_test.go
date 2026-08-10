package monstera

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func TestConfigVersionMetric(t *testing.T) {
	n := &Node{nodeId: "config_version_metric_node"}

	n.setConfigVersionMetric(7)
	if got := testutil.ToFloat64(configVersion.WithLabelValues(n.nodeId)); got != 7 {
		t.Fatalf("configVersion = %v, want 7", got)
	}

	// A later config bump updates the same series.
	n.setConfigVersionMetric(9)
	if got := testutil.ToFloat64(configVersion.WithLabelValues(n.nodeId)); got != 9 {
		t.Fatalf("configVersion = %v, want 9", got)
	}

	// Without an id (unprovisioned node) it is a no-op and creates no series.
	(&Node{}).setConfigVersionMetric(3)
}

func TestReplicaCommitLagMetric(t *testing.T) {
	nodeID := "replica_commit_lag_node"

	// lagSeries gathers the current commit-lag series for this node id only, so
	// the assertions are isolated from series other tests leave on the global vec.
	lagSeries := func() map[lagLabels]float64 {
		reg := prometheus.NewRegistry()
		require.NoError(t, reg.Register(replicaCommitLag))
		mfs, err := reg.Gather()
		require.NoError(t, err)

		out := map[lagLabels]float64{}
		for _, mf := range mfs {
			if mf.GetName() != "monstera_raft_replica_commit_lag" {
				continue
			}
			for _, m := range mf.GetMetric() {
				labels := map[string]string{}
				for _, lp := range m.GetLabel() {
					labels[lp.GetName()] = lp.GetValue()
				}
				if labels["node"] != nodeID {
					continue
				}
				out[lagLabels{
					application: labels["application"],
					shard:       labels["shard"],
					replica:     labels["replica"],
				}] = m.GetGauge().GetValue()
			}
		}
		return out
	}

	a := lagLabels{application: "app", shard: "s1", replica: "r-a"}
	b := lagLabels{application: "app", shard: "s2", replica: "r-b"}

	// First tick: publish two replicas with different lag.
	published := publishReplicaLag(nodeID, []replicaLagSample{
		{labels: a, lag: 5},
		{labels: b, lag: 0},
	}, nil)

	got := lagSeries()
	require.Equal(t, 5.0, got[a])
	require.Equal(t, 0.0, got[b])
	require.Len(t, got, 2)

	// Second tick: replica b is gone, a's lag changed. b's series must be dropped.
	published = publishReplicaLag(nodeID, []replicaLagSample{
		{labels: a, lag: 2},
	}, published)

	got = lagSeries()
	require.Equal(t, 2.0, got[a])
	require.Len(t, got, 1, "series for a replica no longer hosted must be deleted")

	// Clean up this node's series so the global vec is left as we found it.
	publishReplicaLag(nodeID, nil, published)
	require.Empty(t, lagSeries())
}

func TestRegisterMetrics(t *testing.T) {
	reg := prometheus.NewRegistry()

	// Registering into a fresh registry must not panic: no duplicate metric
	// names across layers, all collectors valid.
	RegisterMetrics(reg)

	// Registering again must panic, proving the metrics were actually registered
	// (MustRegister rejects an already-registered collector).
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic on double registration, got none")
			}
		}()
		RegisterMetrics(reg)
	}()
}
