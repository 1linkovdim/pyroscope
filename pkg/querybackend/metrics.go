package querybackend

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	datasetTenantIsolationFailure prometheus.Counter
	blockStageDuration            *prometheus.HistogramVec
	blockWorkItems                *prometheus.HistogramVec
	blockWorkBytes                *prometheus.HistogramVec
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
		datasetTenantIsolationFailure: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "dataset_tenant_isolation_failure_total",
			}),
		blockStageDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "block_stage_duration_seconds",
				Help:      "Per-block wall time for total, index lookup, and dataset wait; summed stage times for dataset open, tree processing, and asynchronous symbol partition work. Sums can overlap wall time.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
			},
			[]string{"stage"},
		),
		blockWorkItems: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "block_work_items",
				Help:      "Per-block tree-query counts by fixed kind: queries, profile rows, candidate samples, symbol partitions, and unique stacktrace IDs.",
				Buckets:   []float64{0, 1, 4, 16, 64, 256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304},
			},
			[]string{"kind"},
		),
		blockWorkBytes: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "pyroscope",
				Subsystem: "query_backend",
				Name:      "block_work_bytes",
				Help:      "Per-block estimated section sizes and serialized tree bytes by fixed kind; section sizes are metadata estimates, not fetched bytes.",
				Buckets:   []float64{0, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216, 67108864, 268435456, 1073741824},
			},
			[]string{"kind"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.datasetTenantIsolationFailure, m.blockStageDuration, m.blockWorkItems, m.blockWorkBytes)
	}
	return m
}
