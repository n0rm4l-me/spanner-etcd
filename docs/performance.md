# Performance

Benchmarked on GCP `e2-standard-4` (4 vCPU, 16GB, `asia-northeast1-a`) with production Spanner (`regional-asia-northeast1`, 1000 PU) in the same region — not the emulator.

## Write Throughput — PENDING_COMMIT_TIMESTAMP

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| PUT ×1 | 86 | 11.7ms |
| PUT ×8 | 379 | 2.6ms |
| PUT ×32 | **673** | 1.5ms |

**vs. integer counter (same hardware):** ×1: 3.6× faster · ×8: 6.5× faster · ×32: **15× faster**

## Read Throughput

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| GET ×1 | 71 | 14.0ms |
| GET ×32 | 1,391 | 0.7ms |
| GET ×64 | **1,504** | 0.7ms |

## Kubernetes Workload — kubeadm v1.31 (100 pods, initial validation)

| Metric | Value |
|--------|-------|
| 50 Deployments created (parallel) | 3.75s |
| 100 pods Ready | 44s |
| KV/Range avg latency | ~22ms |

## Production Validation

Tested with **22 production Java/Kotlin microservices** (Vert.x + jetcd) on GKE:

- All services connected and loaded data from spanner-etcd on startup
- 45 active Watch streams across all services
- Auth token expiry (30s TTL) → auto-reauth with zero errors
- Pod kill → 45 Watch streams migrated to surviving replica in ~10s, zero errors
- Watch event delivery confirmed: jetcd received PUT events within ~1s (poll mode)

## Kubernetes v1.33 Soak Test

24-hour soak test on Kubernetes v1.33.12 (kubeadm, single-node, GCP `e2-standard-4`) — in progress.

Load profile:
- Rolling deployment with 1–10 replicas scaled every 2 minutes
- ConfigMap churn every 3 minutes (create + bulk delete)
- cert-manager operator running concurrently
- 57+ active Watch streams throughout

Results will be published after completion.
