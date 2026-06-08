# Changelog

## v0.1.0 — 2026-06-08

First public release.

Replace etcd with Google Cloud Spanner. Drop-in, no client changes.

```
--etcd-servers=http://spanner-etcd:2379
```

### Highlights

- **`PENDING_COMMIT_TIMESTAMP` revision** — no shared counter, no write lock. 15× higher write throughput at ×32 concurrency vs integer counter baseline
- **Spanner Change Streams** for Watch — ~116ms end-to-end event delivery
- **Atomic Txn** — compare+ops in a single Spanner ReadWriteTransaction
- **Covering index** on `kv(key, rev DESC)` — Get +40%, mixed workload +161% vs non-indexed baseline
- **Stateless replicas** — all state in Spanner, scale out by adding pods

### Validated

- Kubernetes v1.33.12 (kubeadm, external etcd) — 24h soak test, zero crashes, zero data loss
- TLS / mTLS — server and client certificates, plaintext rejected
- Multi-replica HA — Watch continuity across replica failover, zero gaps
- 22 production Java/Kotlin microservices (Vert.x + jetcd) on GKE

### Performance

Production Spanner, `asia-northeast1`, 1000 PU, same-region VM:

| Operation | ops/sec |
|-----------|--------:|
| Create ×1 | 83 |
| Create ×4 parallel | 257 |
| Get ×1 | 100 |
| Get ×4 parallel | 443 |
| Mixed ×4 (70% reads) | 400 |
| Watch latency | ~116ms |

### Known limitations

- Auth RBAC (UserAdd/RoleAdd/GrantPermission) not implemented — not needed for standard Kubernetes
- Watch latency ~100–150ms — inherent to Spanner Change Streams, not suitable for sub-10ms use cases
