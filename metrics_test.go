package monstera

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
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
