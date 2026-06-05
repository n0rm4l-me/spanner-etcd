# spanner-etcd

A **drop-in etcd replacement** backed by **Google Cloud Spanner** — tested with real Kubernetes v1.31 (kubeadm) and production application workloads.

Implements the complete etcd v3 KV/Watch/Lease/Auth API. Swap out etcd for spanner-etcd and get unlimited horizontal scale, native multi-region replication, and 99.999% SLA — with zero etcd cluster management.

```
# Before
--etcd-servers=https://etcd-0:2379,https://etcd-1:2379,https://etcd-2:2379

# After
--etcd-servers=http://spanner-etcd:2379
```

## Why

Standard etcd is a single-region, single-cluster system. At Google scale, the GKE team replaced etcd with a Spanner-backed implementation to scale clusters to [65,000+ nodes](https://cloud.google.com/blog/products/containers-kubernetes/gke-65k-nodes-and-counting).

This project does the same thing as an open implementation:

| | etcd | spanner-etcd |
|---|---|---|
| Storage | Raft log on local disk | Google Cloud Spanner |
| Horizontal scale | Limited (3–5 members) | Unlimited replicas |
| Cross-region | Manual federation | Native (Spanner multi-region) |
| Durability | Single-region by default | 99.999% SLA |
| Operations | etcd cluster management | Serverless |

## Architecture

```
Kubernetes API Server (or any etcd client)
         │  etcd v3 gRPC (optional TLS / mTLS)
         ▼
    spanner-etcd
    ┌───────────────────────────────────────┐
    │  KVServer     WatchServer             │
    │  LeaseServer  AuthServer              │
    │  ClusterServer  MaintenanceServer     │
    │           │                           │
    │      SpannerStore                     │
    │   ┌───────────────────────────────┐   │
    │   │  Write: INSERT kv             │   │
    │   │  rev = PENDING_COMMIT_TS()    │   │
    │   │  → no lock, no counter        │   │
    │   │                               │   │
    │   │  Watch: Change Stream reader  │   │
    │   │  (10–50ms) with poll fallback │   │
    │   │  (1s) for emulator            │   │
    │   │                               │   │
    │   │  Lease: TTL goroutine         │   │
    │   └───────────────────────────────┘   │
    └──────────────┬────────────────────────┘
                   │  Spanner gRPC
                   ▼
         Google Cloud Spanner
         ├── kv              (append-only KV log)
         ├── kv_rev          (compact revision only)
         ├── kv_lease        (TTL leases)
         ├── kv_cs_cursors   (Change Stream resume points)
         └── kv_changes      (Change Stream)
```

Multiple `spanner-etcd` replicas can run concurrently — all state lives in Spanner. No consensus, no leader election between replicas.

## Implemented etcd v3 API

| Service | Method | Status | Notes |
|---------|--------|--------|-------|
| **KV** | Range (Get/List) | ✅ | Single key, prefix, range, historical (rev=N), count-only |
| **KV** | Put | ✅ | Create + unconditional update |
| **KV** | DeleteRange | ✅ | Single key and prefix |
| **KV** | Txn | ✅ | Compare-and-swap: MOD, VERSION, CREATE, VALUE operators |
| **KV** | Compact | ✅ | Async GC of old revisions |
| **Watch** | Watch | ✅ | Live streaming, prefix filter, revision replay, PrevKv |
| **Lease** | LeaseGrant/Revoke | ✅ | TTL leases with immediate key deletion on revoke |
| **Lease** | LeaseKeepAlive | ✅ | Bidirectional streaming keepalive |
| **Lease** | LeaseTimeToLive | ✅ | TTL query |
| **Auth** | Authenticate | ✅ | Username/password → token; clients auto-re-authenticate on expiry |
| **Auth** | AuthEnable/Disable/Status | ✅ | Stubs — auth controlled via `--auth-users` flag |
| **Cluster** | MemberList | ✅ | Returns self as single member |
| **Maintenance** | Status | ✅ | Returns current revision |
| gRPC Health | Check | ✅ | Standard Kubernetes liveness probe |
| gRPC Health | `/healthz` HTTP | ✅ | Kubernetes readiness/liveness probe on metrics port |

## Spanner Schema

```sql
-- See ddl/schema.sql for the full DDL.

-- rev = PENDING_COMMIT_TIMESTAMP() on every write.
-- No shared counter row, no lock — each transaction is fully independent.
CREATE TABLE kv (
  id               INT64     NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE kv_seq)),
  rev              TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true),
  key              STRING(2048) NOT NULL,
  value            BYTES(MAX),
  old_value        BYTES(MAX),
  lease_id         INT64,
  deleted          BOOL NOT NULL DEFAULT (false),
  created          BOOL NOT NULL DEFAULT (false),
  create_revision  TIMESTAMP OPTIONS (allow_commit_timestamp = true),
  prev_revision    TIMESTAMP OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (id);

-- kv_rev stores only the compact revision (not the current revision).
-- Current revision = MAX(rev) FROM kv — no lock needed.
CREATE TABLE kv_rev (
  id  INT64     NOT NULL,
  rev TIMESTAMP NOT NULL
) PRIMARY KEY (id);
```

### Design decisions

**`PENDING_COMMIT_TIMESTAMP()` as revision**: Every write sets `rev = PENDING_COMMIT_TIMESTAMP()` — Spanner's TrueTime-based commit timestamp. No shared counter row, no lock. Each transaction is fully independent. etcd clients receive `rev` as `int64` (UnixNano), which is a valid etcd `ModRevision`. This eliminates the serialization bottleneck of integer counters and provides **15× higher write throughput** at ×32 concurrency.

**`id` vs `rev`**: Physical PK (`id`) uses `bit_reversed_positive` to distribute writes across Spanner splits and avoid hotspots.

**Append-only log**: Like etcd, rows in `kv` are never updated — each write appends a new row. Compaction physically deletes old rows asynchronously.

**Change Streams for Watch**: Each replica streams all partitions of `kv_changes`. Spanner pushes records as writes commit (~10–50ms). Partition cursors are persisted every 5s so replicas resume from the correct position after restart. The poll loop (1s) runs as a fallback when Change Streams are unavailable (Spanner emulator).

## Performance

Benchmarked on GCP `e2-standard-4` (4 vCPU, 16GB, `asia-northeast1-a`) with production Spanner (`regional-asia-northeast1`, 1000 PU) in the same region — not the emulator.

### Write throughput — PENDING_COMMIT_TIMESTAMP

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| PUT ×1 | 86 | 11.7ms |
| PUT ×8 | 379 | 2.6ms |
| PUT ×32 | **673** | 1.5ms |

**vs. integer counter (same hardware):** ×1: 3.6× faster · ×8: 6.5× faster · ×32: **15× faster**

### Read throughput

| Clients | ops/sec | Avg latency |
|---------|---------|-------------|
| GET ×1 | 71 | 14.0ms |
| GET ×32 | 1,391 | 0.7ms |
| GET ×64 | **1,504** | 0.7ms |

### Kubernetes workload — kubeadm v1.31 (100 pods)

| Metric | Value |
|--------|-------|
| 50 Deployments created (parallel) | 3.75s |
| 100 pods Ready | 44s |
| KV/Range avg latency | ~22ms |

## Installation

### Prerequisites

- Go 1.21+
- Google Cloud project with Spanner enabled
- A Spanner instance and database

```bash
git clone https://github.com/n0rm4l-me/spanner-etcd
cd spanner-etcd
go mod vendor
go build -o spanner-etcd ./cmd/server/
```

## Configuration

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--spanner-database` | `SPANNER_DATABASE` | — | **Required.** Full path: `projects/P/instances/I/databases/D` |
| `--listen-address` | `LISTEN_ADDR` | `0.0.0.0:2379` | gRPC listen address |
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:2381` | HTTP `/metrics` and `/healthz` |
| `--tls-cert` | `TLS_CERT` | — | Server TLS certificate |
| `--tls-key` | `TLS_KEY` | — | Server TLS private key |
| `--tls-ca` | `TLS_CA` | — | CA cert for mTLS client verification |
| `--auth-users` | `ETCD_AUTH_USERS` | — | `user1:pass1,user2:pass2` — empty = auth disabled |
| `--auth-token-ttl` | — | `5m` | Token lifetime; clients auto-re-authenticate on expiry |
| `--log-level` | `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--spanner-native-metrics` | — | `false` | Enable Spanner client-side metrics (requires `roles/monitoring.metricWriter`) |

## Quick Start

### 1. With Spanner Emulator (local development)

```bash
# Start Spanner emulator
docker run -d -p 9010:9010 -p 9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator

# Create instance and database
curl -X POST http://localhost:9020/v1/projects/my-project/instances \
  -d '{"instanceId":"my-instance","instance":{"config":"emulator-config","displayName":"dev","nodeCount":1}}'

curl -X POST http://localhost:9020/v1/projects/my-project/instances/my-instance/databases \
  -d '{"createStatement":"CREATE DATABASE `my-db`"}'

# Start spanner-etcd
SPANNER_EMULATOR_HOST=localhost:9010 spanner-etcd \
  --spanner-database=projects/my-project/instances/my-instance/databases/my-db

# Test
etcdctl put /hello world
etcdctl get /hello
```

### 2. Production with authentication

```bash
spanner-etcd \
  --spanner-database=projects/MY_PROJECT/instances/INSTANCE/databases/DB \
  --auth-users="root:strongpassword" \
  --auth-token-ttl=24h

# Client (etcdctl)
etcdctl --endpoints=http://spanner-etcd:2379 \
  --user=root:strongpassword \
  put /key value

# Client (jetcd / Java)
# jetcd automatically re-authenticates when token expires — no client changes needed
```

### 3. Production with mTLS

```bash
spanner-etcd \
  --spanner-database=... \
  --tls-cert=server.crt \
  --tls-key=server.key \
  --tls-ca=ca.crt   # enables mutual TLS — clients must present a cert
```

### 4. As Kubernetes etcd backend (kubeadm)

Tested with **Kubernetes v1.31.14 (kubeadm)** — full control plane, CoreDNS, Flannel CNI, Deployments, pods — all working.

```yaml
# kubeadm-config.yaml
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
etcd:
  external:
    endpoints:
      - http://127.0.0.1:2379   # spanner-etcd as systemd service
```

```bash
kubeadm init --config=kubeadm-config.yaml \
  --ignore-preflight-errors=ExternalEtcdVersion
```

> **Note**: seed initial revision to 1 — Kubernetes API server rejects `revision=0`.
> spanner-etcd does this automatically on first startup.

### 5. Deploy on Kubernetes (Helm)

```bash
helm install spanner-etcd ./helm/spanner-etcd \
  --set spannerDatabase=projects/P/instances/I/databases/D \
  --set auth.users="root:secret" \
  --set auth.tokenTTL="24h"
```

### 6. Migrate data from existing etcd

```bash
# One-time migration via Helm post-install Job:
helm upgrade spanner-etcd ./helm/spanner-etcd \
  --set spannerDatabase=... \
  --set migrate.enabled=true \
  --set migrate.src=http://etcd.my-namespace:2379 \
  --set migrate.srcUser=root \
  --set migrate.srcPasswordSecret=etcd-secret \
  --set migrate.verify=true

# Or run the migrate binary directly inside the cluster:
kubectl run migrate --image=...spanner-etcd:VERSION \
  --command -- /spanner-etcd-migrate \
    --src=http://etcd:2379 --src-user=root \
    --dst=http://spanner-etcd:2379 --verify
```

## Schema Management

`spanner-etcd` applies DDL on startup. If the runtime SA lacks DDL permissions, a warning is logged and the server continues (schema managed externally).

```bash
# Apply schema manually (production — admin runs once):
gcloud spanner databases ddl update MY_DB \
  --instance=MY_INSTANCE --project=MY_PROJECT \
  --ddl-file=./ddl/schema.sql

# Runtime SA needs only:
roles/spanner.databaseUser   # on the database
roles/iam.workloadIdentityUser  # for WIF
```

## Horizontal Scaling

All replicas are stateless — all state lives in Spanner.

```
                    LoadBalancer :2379
                   /              \
          spanner-etcd-1    spanner-etcd-2
                   \              /
                Google Cloud Spanner
```

On pod restart, Watch clients reconnect automatically via the etcd client retry logic. With `preStop: sleep 15s`, Kubernetes removes the pod from Service endpoints before SIGTERM — Watch streams migrate to surviving replicas with zero errors.

**Graceful shutdown sequence:**

```
t=0    preStop sleep (15s) — endpoint propagation
t=15s  SIGTERM → GracefulStop (10s timeout for in-flight RPCs)
t=25s  Force stop if needed
t=25s  Spanner connections closed
```

Tested: 45 Watch streams migrated to the surviving replica in ~10s, zero application errors.

## Multi-Region Setup

Spanner supports multi-region instances out of the box — no code changes needed.

| Config | Coverage | Write latency |
|--------|----------|--------------|
| `regional-us-central1` | Single region | ~5ms |
| `nam4` | US East + West | ~10ms |
| `eur3` | EU West + North | ~10ms |
| `asia1` | Asia East + West | ~10ms |
| `nam-eur-asia1` | US + EU + Asia | ~30ms |

RPO=0 for all multi-region configs — synchronous replication, zero data loss if a region fails.

### Processing units

| Cluster size | Recommended PUs |
|---|---|
| < 100 nodes | 100 PUs |
| 100–1000 nodes | 500–1000 PUs |
| 1000–10000 nodes | 1000–3000 PUs |
| 65000 nodes (GKE scale) | 5000+ PUs |

## Monitoring

`/metrics` (Prometheus) and `/healthz` are served on `--metrics-addr` (default `:2381`).

Key metrics:

| Metric | Description |
|--------|-------------|
| `spanner_etcd_active_watches` | Active Watch subscriptions |
| `spanner_etcd_change_stream_active` | 1 = CS delivering events; 0 = poll fallback |
| `spanner_etcd_change_stream_partitions_active` | CS partitions being read |
| `spanner_etcd_current_revision` | Latest revision (UnixNano) |
| `spanner_etcd_rpc_duration_seconds` | gRPC latency histogram by method |
| `spanner_etcd_active_leases` | Active TTL leases |

**GKE Managed Prometheus**: `PodMonitoring` is included in the Helm chart (`podMonitoring.enabled: true`).

### Structured logging (Google Cloud Logging)

```json
{"severity":"INFO","time":"2026-06-05T10:00:00Z","caller":"server/main.go:54","message":"spanner-etcd listening","addr":"0.0.0.0:2379"}
{"severity":"INFO","time":"...","message":"authenticated","user":"root","client":"10.244.0.15:52341"}
{"severity":"WARNING","time":"...","message":"partition read error, retrying"}
```

## Known Limitations

### Auth tokens are per-replica

Auth tokens are stored in memory. A client that connected to replica-1 and switches to replica-2 receives `UNAUTHENTICATED` and re-authenticates automatically (tested with jetcd). No data is lost — only one extra round-trip on reconnect.

### Watch fan-out at extreme scale

With 10,000+ watchers and 1,000 writes/sec, the synchronous `dispatchEvents` becomes a goroutine scheduling bottleneck. Mitigation: add more replicas — each handles an independent subset of Watch connections.

### Change Streams not supported on Spanner emulator

The emulator does not support the `READ_kv_changes` TVF. spanner-etcd falls back to 1-second polling automatically. Watch latency on the emulator is ~1s; on production Spanner it is ~10–50ms.

### Not implemented

- Auth RBAC (UserAdd/RoleAdd/GrantPermission) — not needed for standard Kubernetes
- Defrag / Snapshot — not needed (Spanner manages storage automatically)

## Why not kine?

[kine](https://github.com/k3s-io/kine) works well with PostgreSQL and MySQL but is a poor fit for Spanner: its `generic.Dialect` assumes `MAX(id)` equals the global revision (breaks with `bit_reversed_positive` sequences), relies on `LIKE ... ESCAPE` (unsupported), and uses reserved word aliases (`AS current`, `AS compact`). Almost every query needs overriding — at which point implementing `server.Backend` directly is cleaner.

## Testing

48 integration tests against the Spanner emulator.

```bash
SPANNER_EMULATOR_HOST=localhost:9010 go test ./...
```

| Package | Tests | What's covered |
|---------|-------|----------------|
| `pkg/store` | 25 | Create/Get/Update/Delete/List/Count/After/Compact/Watch/Lease/Lease+Watch |
| `pkg/server` | 13 + 3 auth | Full gRPC stack, Txn multi-op, Watch cancel/fanout/concurrent, graceful shutdown, auth token expiry / re-auth |

## Production validation

Tested with **22 production Java/Kotlin microservices** (Vert.x + jetcd) on GKE:

- All services connected and loaded data from spanner-etcd on startup
- 45 active Watch streams across all services
- Auth token expiry (30s TTL) → auto-reauth with zero errors
- Pod kill → 45 Watch streams migrated to surviving replica in ~10s, zero errors
- Watch event delivery confirmed: jetcd received PUT events within ~1s (poll mode)

## Roadmap

- [x] PENDING_COMMIT_TIMESTAMP revision (no serialization bottleneck, 15× write speedup)
- [x] Spanner Change Streams for Watch (~10–50ms latency on production Spanner)
- [x] Simple username/password authentication with auto-reauth
- [x] Prometheus metrics + GKE PodMonitoring
- [x] Helm chart with WIF, PDB, HPA, graceful shutdown (preStop + GracefulStop)
- [x] Kubernetes v1.31 (kubeadm) — full control plane tested
- [x] Production validation: 22 microservices, 45 Watch streams
- [x] 38 integration tests (emulator)
- [ ] Auth RBAC (UserAdd/RoleGrantPermission)
- [ ] Change Streams support on Spanner emulator

## Development

```bash
# Build local binary
make build

# Vendor dependencies (required before Docker build)
make vendor

# Build Docker image (linux/amd64)
make docker VERSION=0.4.0

# Build + push
make release VERSION=0.4.0

# Run tests
make test

# Run with race detector
SPANNER_EMULATOR_HOST=localhost:9010 \
go run -race ./cmd/server/ \
  --spanner-database=projects/test/instances/test/databases/test \
  --log-level=debug
```

> **Note on vendor**: `vendor/` is not committed. Run `make vendor` before `make docker`.
> In corporate networks with custom CA certificates, `go mod download` inside Docker
> fails — vendoring locally avoids this.

## License

Apache 2.0
