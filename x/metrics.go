package monsterax

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func MeasureSince(o prometheus.Observer, t1 time.Time) {
	o.Observe(time.Since(t1).Seconds())
}
