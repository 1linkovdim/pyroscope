package querybackend

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	datasetTenantIsolationFailure prometheus.Counter
	blockStageDuration            *prometheus.HistogramVec
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
				Help:      "Per-block wall time for total, index lookup, and dataset wait; summed per-dataset time for open, query, tree-query substeps, aggregate, and close.",
				Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10},
			},
			[]string{"stage"},
		),
	}
	if reg != nil {
		reg.MustRegister(m.datasetTenantIsolationFailure, m.blockStageDuration)
	}
	return m
}
