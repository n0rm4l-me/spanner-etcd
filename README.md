# spanner-etcd

A Kubernetes-native **etcd v3 API server** backed by **Google Cloud Spanner**.

Implements the full etcd v3 gRPC protocol so that any etcd client or Kubernetes API server can use it as a drop-in replacement for etcd — with Spanner handling storage, replication, and horizontal scalability.

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
         │  etcd v3 gRPC (TLS / mTLS)
         ▼
    spanner-etcd
    ┌───────────────────────────────────────┐
    │  KVServer     WatchServer             │
    │  LeaseServer  ClusterServer           │
    │  MaintenanceServer                    │
    │           │                           │
    │      SpannerStore                     │
    │   ┌───────────────────────────────┐   │
    │   │  Write: RW txn                │   │
    │   │  UPDATE kv_rev THEN RETURN    │   │
    │   │  + INSERT INTO kv             │   │
    │   │                               │   │
    │   │  Watch: Change Stream reader  │   │
    │   │  (10–50ms) with poll fallback │   │
    │   │  (1s) for emulator/older DBs  │   │
    │   │                               │   │
    │   │  Lease: TTL goroutine         │   │
    │   └───────────────────────────────┘   │
    └──────────────┬────────────────────────┘
                   │  Spanner gRPC
                   ▼
         Google Cloud Spanner
         ├── kv table
         ├── kv_rev (revision counter)
         ├── kv_lease (TTL leases)
         ├── kv_cs_cursors (CS resume points)
         └── kv_changes (Change Stream)
```

Multiple `spanner-etcd` replicas can run concurrently — all state lives in Spanner. No consensus, no leader election between replicas.

## Implemented etcd v3 API

| Service | Method | Status | Notes |
|---------|--------|--------|-------|
| **KV** | Range (Get/List) | ✅ | Single key, prefix, range, historical (rev=N), count-only |
| **KV** | Put | ✅ | Create + unconditional update |
| **KV** | DeleteRange | ✅ | Direct single key + prefix, and via Txn (Kubernetes-style) |
| **KV** | Txn | ✅ | Compare-and-swap, all operators: MOD, VERSION, CREATE, VALUE |
| **KV** | Compact | ✅ | Async GC of old revisions |
| **Watch** | Watch | ✅ | Live streaming, prefix filter, revision replay, PrevKv, progress notify |
| **Lease** | LeaseGrant | ✅ | TTL leases |
| **Lease** | LeaseRevoke | ✅ | Immediate key deletion |
| **Lease** | LeaseKeepAlive | ✅ | Bidirectional streaming keepalive |
| **Lease** | LeaseTimeToLive | ✅ | TTL query |
| **Cluster** | MemberList | ✅ | Returns self as single member |
| **Maintenance** | Status | ✅ | Returns current revision, used as health check |
| gRPC Health | Check | ✅ | Standard Kubernetes liveness probe |

## Spanner Schema

```sql
-- Main KV log: append-only, one row per write event.
-- Physical PK uses bit_reversed_positive to avoid write hotspots.
CREATE TABLE kv (
  id               INT64 NOT NULL DEFAULT (GET_NEXT_SEQUENCE_VALUE(SEQUENCE kv_seq)),
  rev              INT64 NOT NULL,          -- logical monotonic revision
  key              STRING(2048) NOT NULL,
  value            BYTES(MAX),
  old_value        BYTES(MAX),              -- populated for Watch PrevKv
  lease_id         INT64,
  deleted          BOOL NOT NULL DEFAULT (false),
  created          BOOL NOT NULL DEFAULT (false),
  create_revision  INT64 NOT NULL DEFAULT (0),
  prev_revision    INT64 NOT NULL DEFAULT (0)
) PRIMARY KEY (id);

-- Single-row monotonic revision counter.
-- Incremented atomically via UPDATE...THEN RETURN in every write transaction.
CREATE TABLE kv_rev (
  id   INT64 NOT NULL,
  rev  INT64 NOT NULL
) PRIMARY KEY (id);

-- Active leases with TTL.
CREATE TABLE kv_lease (
  lease_id   INT64 NOT NULL,
  ttl_sec    INT64 NOT NULL,
  granted_at TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (lease_id);

-- Change Stream: captures all mutations to kv for Watch delivery.
CREATE CHANGE STREAM kv_changes FOR kv
  OPTIONS (retention_period = '7d', value_capture_type = 'NEW_ROW');

-- Per-replica cursor store: enables resume after restart.
CREATE TABLE kv_cs_cursors (
  replica_id       STRING(128) NOT NULL,
  partition_token  STRING(MAX) NOT NULL,
  resume_timestamp TIMESTAMP  NOT NULL,
  updated_at       TIMESTAMP  NOT NULL OPTIONS (allow_commit_timestamp = true)
) PRIMARY KEY (replica_id, partition_token);
```

### Design decisions

**`id` vs `rev`**: The physical PK (`id`) uses `bit_reversed_positive` to distribute writes across Spanner splits and avoid hotspots. The logical revision (`rev`) is a monotonic integer from `kv_rev`. etcd clients see `rev` as `ModRevision`.

**Atomic revision bump**: Every write does `UPDATE kv_rev SET rev = rev + 1 WHERE id = 1 THEN RETURN rev` — a single server-side DML statement that both increments and reads the counter, avoiding the two-RPC read-modify-write pattern that serialises concurrent transactions.

**Append-only log**: Like etcd, we never update rows in `kv`. Each write appends a new row. Compaction physically deletes old rows asynchronously.

**Change Streams for Watch**: Instead of polling every second, each replica opens a long-lived streaming SQL query per partition of `kv_changes`. Spanner pushes records as writes commit (~10–50ms). The initial query uses `NULL` partition token to discover active partitions; subsequent queries use tokens returned in `ChildPartitionsRecord` entries. Partition cursors are flushed to `kv_cs_cursors` every 5s so replicas resume from the correct position after a restart.

The poll loop (1s) runs in parallel as a safety net during the Change Stream startup window (first 10s) and falls back automatically when Change Streams are unavailable (Spanner emulator does not support the TVF).

**Change Stream SQL syntax** (real Spanner only):
```sql
-- Initial: NULL token discovers partitions
SELECT ChangeRecord FROM READ_kv_changes(@start, NULL, NULL, @heartbeat_ms)

-- Per-partition streaming read (token from ChildPartitionsRecord)
SELECT ChangeRecord FROM READ_kv_changes(@start, NULL, @token, @heartbeat_ms)
```
Must be executed via `client.Single().Query()` — Spanner rejects CS queries on regular `ExecuteSql`.

## Performance

| Operation | Latency (Spanner prod) | Latency (emulator) |
|-----------|----------------------|-------------------|
| Get (single key) | ~5ms | ~20ms |
| Put (create) | ~10ms | ~100ms |
| List (prefix, 100 keys) | ~10ms | ~30ms |
| Watch event delivery (Change Streams) | **~10–50ms** | ~1s (emulator: poll fallback) |
| Watch event delivery (poll fallback) | ~1s | ~1s |

Write latency is bounded by one Spanner RW transaction: `UPDATE kv_rev THEN RETURN` + `INSERT INTO kv`. Both are buffered and committed atomically.

Watch latency with Change Streams is ~10–50ms because Spanner pushes DataChangeRecords as soon as a write commits. The poll fallback (1s) is used automatically on the Spanner emulator or when Change Streams are not available.

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
| `--spanner-database` | `SPANNER_DATABASE` | — | **Required.** Full Spanner DB path: `projects/P/instances/I/databases/D` |
| `--listen-address` | `LISTEN_ADDR` | `0.0.0.0:2379` | gRPC listen address |
| `--metrics-addr` | `METRICS_ADDR` | `0.0.0.0:2381` | HTTP address for `/metrics` and `/healthz` |
| `--tls-cert` | `TLS_CERT` | — | Server TLS certificate file |
| `--tls-key` | `TLS_KEY` | — | Server TLS private key file |
| `--tls-ca` | `TLS_CA` | — | CA certificate for mTLS client verification |
| `--log-level` | `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--peer-urls` | `PEER_URLS` | — | Peer URLs advertised in MemberList (comma-separated) |
| `--spanner-native-metrics` | — | `false` | Enable Spanner built-in client metrics (requires `roles/monitoring.metricWriter`) |

## Schema Management

`spanner-etcd` applies DDL on startup via `UpdateDatabaseDdl`. If the runtime service account has only `roles/spanner.databaseUser` (no DDL rights), the server logs a warning and continues — assuming the schema was created externally. This is the recommended production setup:

```bash
# Run once by an admin with roles/spanner.databaseAdmin or roles/spanner.admin:
gcloud spanner databases ddl update MY_DATABASE \
  --instance=MY_INSTANCE \
  --project=MY_PROJECT \
  --ddl-file=./ddl/schema.sql
```

The runtime SA needs only `roles/spanner.databaseUser` on the database plus `roles/iam.workloadIdentityUser` for WIF.

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

# Start spanner-etcd (no TLS for local dev)
SPANNER_EMULATOR_HOST=localhost:9010 spanner-etcd \
  --spanner-database=projects/my-project/instances/my-instance/databases/my-db

# Test with etcdctl
etcdctl put /hello world
etcdctl get /hello
```

### 2. Production with TLS

```bash
# Generate certificates (or use cert-manager)
openssl req -x509 -newkey rsa:4096 -nodes -days 3650 \
  -keyout ca.key -out ca.crt -subj "/CN=spanner-etcd-ca"

# Server cert with SANs
openssl req -newkey rsa:4096 -nodes -keyout server.key \
  -out server.csr -subj "/CN=spanner-etcd"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 3650 \
  -extfile <(echo "subjectAltName=DNS:localhost,IP:127.0.0.1")

# Start with mTLS
spanner-etcd \
  --spanner-database=projects/my-project/instances/prod/databases/k8s \
  --tls-cert=server.crt \
  --tls-key=server.key \
  --tls-ca=ca.crt

# Test
etcdctl --endpoints=https://localhost:2379 \
  --cacert=ca.crt --cert=client.crt --key=client.key \
  endpoint health
```

### 3. As Kubernetes etcd backend (k3s)

```bash
# k3s with spanner-etcd as external datastore
k3s server \
  --datastore-endpoint="https://spanner-etcd:2379" \
  --datastore-cafile="/etc/spanner-etcd/ca.crt" \
  --datastore-certfile="/etc/spanner-etcd/client.crt" \
  --datastore-keyfile="/etc/spanner-etcd/client.key" \
  --disable-cloud-controller
```

## Horizontal Scaling

Run multiple `spanner-etcd` replicas behind a load balancer:

```
                    LoadBalancer :2379
                   /              \
          spanner-etcd-1    spanner-etcd-2
                   \              /
                Google Cloud Spanner
```

All replicas are stateless — no coordination needed. Spanner guarantees external consistency (linearizability) across all replicas. Each replica independently reads the `kv_changes` Change Stream, so Watch events are delivered to clients of any replica within ~10–50ms of the write committing.

## Monitoring

`spanner-etcd` exposes standard gRPC health check on the same port:

```bash
grpc_health_probe -addr=localhost:2379 -tls \
  -tls-ca-cert=ca.crt -tls-client-cert=client.crt -tls-client-key=client.key
```

Slow RPCs (>500ms) are logged at `info` level with method name and elapsed time. Set `--log-level=debug` to log all RPCs.

## Why not kine?

[kine](https://github.com/k3s-io/kine) is a popular etcd shim that translates the etcd API to SQL. It works well with PostgreSQL and MySQL, but is a poor fit for Spanner: kine's `generic.Dialect` assumes `MAX(id)` equals the global revision, which breaks with Spanner's `bit_reversed_positive` sequences; it relies on `LIKE ... ESCAPE` which Spanner doesn't support; and its query aliases (`AS current`, `AS compact`) collide with Spanner reserved words. The result is that almost every query needs to be overridden, at which point you're better off implementing the etcd `server.Backend` interface directly — which is what this project does.

## Limitations vs etcd

| Feature | etcd | spanner-etcd |
|---------|------|-------------|
| Watch latency | <10ms | ~10–50ms (Change Streams) |
| Watch latency (emulator) | <10ms | ~1s (poll, CS not supported) |
| Lease keepalive | Streaming | Streaming ✅ |
| DeleteRange (bare gRPC) | ✅ | ✅ Single key + prefix |
| Defrag / Snapshot | ✅ | Not implemented (not needed — Spanner manages storage) |
| Auth (RBAC) | ✅ | Not implemented |

### Roadmap

- [x] Spanner Change Streams for sub-second Watch latency (10–50ms, real Spanner)
- [x] Prometheus metrics (`/metrics` on `:2381`)
- [x] Helm chart with WIF, PDB, HPA, ServiceMonitor
- [x] Deployed and tested on GKE with real Spanner — 33/33 etcd operations pass
- [ ] Unit tests with Spanner emulator
- [ ] etcd auth passthrough
- [ ] Multi-region Spanner configuration examples
- [ ] Spanner Change Streams for emulator (currently uses poll fallback)

## Development

```bash
# Run tests
go test ./...

# Run with race detector
go run -race ./cmd/server/ --spanner-database=...

# Local emulator
SPANNER_EMULATOR_HOST=localhost:9010 go run ./cmd/server/ \
  --spanner-database=projects/test/instances/test/databases/test \
  --log-level=debug
```

## License

Apache 2.0
