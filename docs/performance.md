# Performance

All benchmarks run on GCP `e2-standard-4` (4 vCPU, 16GB, `asia-northeast1-a`) against production Spanner (`regional-asia-northeast1`, 1000 PU) in the same region — not the emulator.

## Write Throughput — PENDING_COMMIT_TIMESTAMP

Benchmarked with the old integer-counter baseline vs PENDING_COMMIT_TIMESTAMP:

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| PUT ×1 | 86 | 11.7ms |
| PUT ×8 | 379 | 2.6ms |
| PUT ×32 | **673** | 1.5ms |

**vs. integer counter (same hardware):** ×1: 3.6× faster · ×8: 6.5× faster · ×32: **15× faster**

## Full Benchmark Suite — Production Spanner (asia-northeast1, 1000 PU)

Benchmarked against a clean database. Results are averaged over 3 runs.
Covering index on `kv(key, rev DESC)` eliminates back-joins on read paths.

| Operation | ops/sec | Avg latency | Notes |
|-----------|---------|-------------|-------|
| Create ×1 | **83** | 12.0ms | Single-key PUT |
| Create ×4 (parallel) | **257** | 3.9ms | PCT eliminates write serialization |
| Update ×1 | **81** | 12.3ms | CAS update |
| Get ×1 | **100** | 10.0ms | Single-key GET |
| Get ×4 (parallel) | **443** | 2.3ms | |
| List 100 keys | **16** | 63ms | Prefix scan, 100 results |
| Mixed ×4 (70% read / 20% write / 10% update) | **400** | 2.5ms | Kubernetes-like workload |

> Performance is dominated by Spanner round-trip latency (~12ms). For best results,
> run spanner-etcd in the same GCP region as your Spanner instance.

## Read Throughput (high concurrency)

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| GET ×1 | 100 | 10.0ms |
| GET ×4 | 443 | 2.3ms |
| GET ×32 | 1,391 | 0.7ms |
| GET ×64 | **1,504** | 0.7ms |

## Spanner Processing Units — Scaling Comparison

Same VM, same database (non-empty, ~40K rows), same benchmark binary. Only Spanner PU changed.

| Operation | 100 PU | 1000 PU | 2000 PU |
|-----------|-------:|--------:|--------:|
| Create ×1 | 85 | 83 | 83 |
| Create ×4 parallel | **134** | **257** | **258** |
| Update ×1 | 84 | 81 | 85 |
| Get ×1 | 105 | 100 | 103 |
| Get ×4 parallel | 475 | 443 | 462 |
| List 100 keys | 12 | 16 | 14 |
| Mixed ×4 (70% read) | **332** | **400** | **414** |
| Watch latency | **33ms** | 116ms | 133ms |

**Key observations:**

- **Single-key ops** are nearly identical across all PU tiers — latency is dominated by network round-trip, not Spanner compute
- **Parallel writes** scale with PU: Create ×4 drops from 257 to 134 ops/sec at 100 PU — the bottleneck shifts to Spanner CPU under concurrent write load
- **Watch latency** is surprisingly best at 100 PU (33ms) — Change Stream delivery is faster when the instance is lightly loaded
- **100 PU is sufficient** for small Kubernetes clusters (< 100 nodes) with moderate write rates; upgrade to 1000 PU for larger clusters or sustained parallel workloads

## Kubernetes v1.33 — 24h Soak Test

**Environment:** Kubernetes v1.33.12 (kubeadm, single-node), GCP `e2-standard-4`, production Spanner `regional-asia-northeast1` (1000 PU).

**Duration:** 24 hours continuous

**Load profile:**
- Rolling deployment scaled 1–10 replicas every 2 minutes
- ConfigMap churn every 3 minutes (create + bulk delete)
- cert-manager operator running concurrently
- 57 active Watch streams throughout

**Results:**

| Metric | Value |
|--------|-------|
| Critical errors (panic / unimplemented) | **0** |
| spanner-etcd uptime | **28h** (survived full test + data collection) |
| Change Stream active | **100%** of test duration |
| Active Watch streams | **57** stable |
| Total Txn operations | **185,952** |
| Total KV/Range operations | **237,312** |
| Auto-compaction | **140,924 rows** cleaned |
| Avg Txn latency | **~18.6ms** |
| Kubernetes node status | **Ready** throughout |

**Conclusion:** Zero crashes, zero data loss, zero unimplemented errors over 24 hours with a production Kubernetes v1.33 control plane.

## Kubernetes Workload — kubeadm v1.31 (initial validation)

| Metric | Value |
|--------|-------|
| 50 Deployments created (parallel) | 3.75s |
| 100 pods Ready | 44s |
| KV/Range avg latency | ~22ms |

## Production Validation — GKE

Tested with **22 production Java/Kotlin microservices** (Vert.x + jetcd) on GKE:

- All services connected and loaded data from spanner-etcd on startup
- 45 active Watch streams across all services
- Auth token expiry (30s TTL) → auto-reauth with zero errors
- Pod kill → 45 Watch streams migrated to surviving replica in ~10s, zero errors
- Watch event delivery confirmed: jetcd received PUT events within ~1s (poll mode)
