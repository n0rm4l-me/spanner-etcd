# Changelog

## v0.1.0 — 2026-06-08

First public release.

Replace etcd with Google Cloud Spanner. Drop-in, no client changes.

```
--etcd-servers=http://spanner-etcd:2379
```

### Highlights

- **`PENDING_COMMIT_TIMESTAMP` revision** — no shared counter, no write lock. 15× higher write throughput at ×32 concurrency vs integer counter baseline
- **Spanner Change Streams** for Watch — ~30ms end-to-end event delivery
- **Atomic Txn** — compare+ops in a single Spanner ReadWriteTransaction
- **Covering index** on `kv(key, rev DESC)` — Get +52%, mixed workload +169% vs non-indexed baseline
- **Stateless replicas** — all state in Spanner, scale out by adding pods

### Validated

- Kubernetes v1.33.12 (kubeadm, external etcd) — 24h soak test, zero crashes, zero data loss
- TLS / mTLS — server and client certificates, plaintext rejected
- Multi-replica HA — Watch continuity across replica failover, zero gaps
- 22 production Java/Kotlin microservices (Vert.x + jetcd) on GKE

### Performance

Production Spanner, `us-central1`, 1000 PU, same-region VM:

| Operation | ops/sec |
|-----------|--------:|
| Create ×1 | 90 |
| Create ×4 parallel | 270 |
| Get ×1 | 108 |
| Get ×4 parallel | 481 |
| Mixed ×4 (70% reads) | 403 |
| Watch latency | ~30ms |

### Known limitations

- Auth RBAC (UserAdd/RoleAdd/GrantPermission) not implemented — not needed for standard Kubernetes
- Watch latency ~30ms — inherent to Spanner Change Streams, not suitable for sub-10ms use cases
