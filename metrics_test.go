package monstera

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

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
