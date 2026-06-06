// Package metrics defines and registers all Prometheus metrics for spanner-etcd.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// ── gRPC operations ──────────────────────────────────────────────────────

	// RPCDuration tracks latency for every gRPC method.
	// Labels: method (e.g. "KV/Range"), status ("ok" | "error")
	RPCDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "spanner_etcd",
		Name:      "rpc_duration_seconds",
		Help:      "gRPC request duration in seconds.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 14), // 1ms → 8s
	}, []string{"method", "status"})

	// ── KV operations ─────────────────────────────────────────────────────────

	// KVOperationsTotal counts Create/Update/Delete/Get/List/Txn/Compact.
	KVOperationsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "kv_operations_total",
		Help:      "Total number of KV operations.",
	}, []string{"operation", "status"}) // operation: create|update|delete|get|list|txn|compact

	// CurrentRevision is the latest global revision seen by this replica.
	CurrentRevision = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spanner_etcd",
		Name:      "current_revision",
		Help:      "Current global revision (latest rev in kv_rev).",
	})

	// ── Watch ─────────────────────────────────────────────────────────────────

	// ActiveWatches is the number of active Watch subscriptions on this replica.
	ActiveWatches = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spanner_etcd",
		Name:      "active_watches",
		Help:      "Number of active Watch subscriptions.",
	})

	// WatchEventsTotal counts events dispatched to Watch subscribers.
	// Labels: source ("change_stream" | "poll")
	WatchEventsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "watch_events_total",
		Help:      "Total Watch events dispatched to subscribers.",
	}, []string{"source"})

	// WatchSubscriberDropsTotal counts subscribers dropped due to a full channel.
	WatchSubscriberDropsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "watch_subscriber_drops_total",
		Help:      "Watch subscribers dropped because their channel was full.",
	})

	// ── Change Streams ────────────────────────────────────────────────────────

	// CSMode is 1 when Change Streams are active, 0 when using poll fallback.
	CSMode = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spanner_etcd",
		Name:      "change_stream_active",
		Help:      "1 if Change Stream is the active event source, 0 if using poll fallback.",
	})

	// CSActivePartitions is the number of Change Stream partitions being read.
	CSActivePartitions = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spanner_etcd",
		Name:      "change_stream_partitions_active",
		Help:      "Number of Change Stream partitions currently being read.",
	})

	// CSPartitionRestarts counts how many times a partition reader restarted due to an error.
	CSPartitionRestarts = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "change_stream_partition_restarts_total",
		Help:      "Total Change Stream partition reader restarts due to errors.",
	})

	// ── Leases ────────────────────────────────────────────────────────────────

	// ActiveLeases is the number of leases currently held in memory.
	ActiveLeases = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "spanner_etcd",
		Name:      "active_leases",
		Help:      "Number of active TTL leases.",
	})

	// LeaseExpirations counts leases that expired naturally (not revoked).
	LeaseExpirations = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "lease_expirations_total",
		Help:      "Total leases expired via natural TTL.",
	})

	// ── Compaction ───────────────────────────────────────────────────────────

	// CompactedRowsTotal counts rows physically deleted by compaction.
	// Labels: trigger ("manual" | "auto")
	CompactedRowsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "compacted_rows_total",
		Help:      "Total kv rows physically deleted by compaction.",
	}, []string{"trigger"})

	// CompactionDuration tracks how long each compaction run takes.
	// Labels: trigger ("manual" | "auto")
	CompactionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "spanner_etcd",
		Name:      "compaction_duration_seconds",
		Help:      "Time spent in each compaction run.",
		Buckets:   prometheus.ExponentialBuckets(0.1, 2, 10), // 100ms → ~100s
	}, []string{"trigger"})

	// ── Spanner ───────────────────────────────────────────────────────────────

	// SpannerTransactions counts Spanner RW transactions by outcome.
	SpannerTransactions = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "spanner_etcd",
		Name:      "spanner_transactions_total",
		Help:      "Total Spanner read-write transactions.",
	}, []string{"status"}) // status: ok | error | aborted
)
