package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	JobsGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "fencepost",
			Name:      "jobs",
			Help:      "Current count of jobs by queue and state",
		},
		[]string{"queue", "state"},
	)

	EnqueueTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "fencepost",
			Name:      "enqueue_total",
			Help:      "Total enqueued jobs count partitioned by dedup status",
		},
		[]string{"dedup"},
	)

	LeaseLatencySeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "fencepost",
			Name:      "lease_latency_seconds",
			Help:      "Duration of lease calls in seconds",
			Buckets:   prometheus.DefBuckets,
		},
	)

	QueueLagSeconds = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "fencepost",
			Name:      "queue_lag_seconds",
			Help:      "Queue lag in seconds (time leased minus run_at)",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5, 10, 30, 60},
		},
	)

	LeaseExpirationsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "fencepost",
			Name:      "lease_expirations_total",
			Help:      "Total number of expired leases reaped",
		},
	)

	StaleTokenRejectionsTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "fencepost",
			Name:      "stale_token_rejections_total",
			Help:      "Total number of requests rejected due to stale fencing tokens",
		},
	)

	SchedulerIsLeader = promauto.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "fencepost",
			Name:      "scheduler_is_leader",
			Help:      "1 if this instance is the active scheduler leader, 0 otherwise",
		},
	)

	JobAttempts = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "fencepost",
			Name:      "job_attempts",
			Help:      "Distribution of attempts required for jobs to reach terminal state",
			Buckets:   []float64{1, 2, 3, 4, 5, 10},
		},
	)
)
