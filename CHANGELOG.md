# Changelog

## Unreleased

### Upgrade notes (leases)

- **Schema change: `kv_lease.expires_at`.** A new replica applies `ALTER TABLE kv_lease ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP` at startup. If the runtime service account has no DDL permission (schema managed externally), apply the updated [ddl/schema.sql](ddl/schema.sql) **before** rolling out the new version, otherwise every lease operation fails with a missing-column error.
- **Rolling upgrade.** Old replicas do not persist keepalives. A lease that is kept alive only through old replicas is expired by new replicas at its original grant time plus TTL, so during a rolling upgrade some leases can expire early. Clients re-grant a lost lease (jetcd does), so this is a short blip, but keep the upgrade window short.
- `spanner_etcd_active_leases` is now the cluster-wide count of unexpired leases, refreshed about every 15 seconds, not a per-replica in-memory count.
- Library users: `LeaseManager.Get` was removed and `GetTTL` now takes a context and returns an error.

### Fixed

- **Leases were per-replica in-memory state.** Keepalives only extended an in-memory deadline, and leases were loaded from Spanner once at startup from `granted_at + ttl`. With more than one replica or any restart that broke leases in three ways: a keepalive served by a replica that had not granted the lease returned TTL 0 (the client sees the lease as lost); a restarted or newly started replica expired leases that clients had been keeping alive, deleting their keys; and if the replica that granted a lease died, nobody expired it and its keys stayed forever. Leases are now cluster state: `kv_lease.expires_at` (set and compared in Spanner time, so pod clock skew does not matter) is the single source of truth, every keepalive persists it, any replica answers Keepalive and TimeToLive, and every replica reaps expired leases idempotently. Keepalives are not coalesced, so a lease still lives exactly one TTL after the last keepalive, as in etcd. Matters most for jetcd-style clients that depend on lease keepalive; Kubernetes does not keep leases alive.

- **Bare `Put` could be acknowledged without writing anything.** The Put RPC used a create-then-CAS-update sequence. When another writer changed the same key between the read and the update, the CAS failed silently and the client still got a success response carrying someone else's revision. In a regression test with 8 concurrent writers on one key, only 12 of 40 acknowledged Puts were actually written. Put now reads the latest row and inserts the new one in a single Spanner read-write transaction, so concurrent Puts serialize and every acknowledged Put produces exactly one revision. Kubernetes is not affected (it writes through `Txn`); clients that use plain `Put` (etcdctl, jetcd) are.

## v0.2.0 — 2026-06-09

### New

- **Directed reads** (`--spanner-read-location`) — route read operations to a Spanner replica in the specified GCP region. Use with multi-region Spanner instances to reduce read latency from non-leader regions. Writes always go to the leader.
- **Helm**: `spannerReadLocation` value exposes directed reads configuration.

### Performance

Benchmarks on `us-central1`, `e2-standard-4`, production Spanner (1000 PU):

| Operation | ops/sec |
|-----------|--------:|
| Create ×1 | 90 |
| Create ×4 parallel | 270 |
| Get ×1 | 108 |
| Get ×4 parallel | 481 |
| Mixed ×4 (70% reads) | 403 |
| Watch latency | ~30ms |

See [docs/performance.md](docs/performance.md) for full PU scaling and multi-region breakdown.

---

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
