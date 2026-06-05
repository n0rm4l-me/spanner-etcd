# spanner-etcd

A **drop-in etcd replacement** backed by **Google Cloud Spanner** — tested with real Kubernetes v1.31 (kubeadm).

Implements the complete etcd v3 KV/Watch/Lease API required by Kubernetes. Swap out etcd for spanner-etcd and get unlimited horizontal scale, native multi-region replication, and 99.999% SLA — with zero etcd cluster management.

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
| **Auth** | Authenticate | ✅ | Username/password → token; clients auto-re-authenticate on expiry |
| **Auth** | AuthEnable/Disable/Status | ✅ | Stubs (auth controlled via `--auth-users` flag) |
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
-- See ddl/schema.sql for the full DDL.

-- rev = PENDING_COMMIT_TIMESTAMP() on every write — no lock, no counter.
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

-- kv_rev holds only the compact revision.
-- Current revision = MAX(rev) FROM kv (no lock needed).
CREATE TABLE kv_rev (
  id  INT64     NOT NULL,
  rev TIMESTAMP NOT NULL
) PRIMARY KEY (id);
```

### Design decisions

**`PENDING_COMMIT_TIMESTAMP()` as revision**: Every write sets `rev = PENDING_COMMIT_TIMESTAMP()` — Spanner's TrueTime-based commit timestamp. No shared counter row, no lock. Each transaction is fully independent. etcd clients receive `rev` as `int64` (UnixNano), which is a valid etcd `ModRevision`.

**`id` vs `rev`**: Physical PK (`id`) uses `bit_reversed_positive` to distribute writes across Spanner splits and avoid hotspots.

**Append-only log**: Like etcd, we never update rows in `kv`. Each write appends a new row. Compaction physically deletes old rows asynchronously.

**Change Streams for Watch**: Each replica streams all partitions of `kv_changes`. Spanner pushes records as writes commit (~10–50ms). Partition cursors are flushed to `kv_cs_cursors` every 5s for resume after restart. The poll loop (1s) runs as a fallback when Change Streams are unavailable (Spanner emulator).

**Change Stream SQL syntax** (real Spanner only):
```sql
-- Initial: NULL token discovers partitions
SELECT ChangeRecord FROM READ_kv_changes(@start, NULL, NULL, @heartbeat_ms)

-- Per-partition streaming read (token from ChildPartitionsRecord)
SELECT ChangeRecord FROM READ_kv_changes(@start, NULL, @token, @heartbeat_ms)
```
Must be executed via `client.Single().Query()` — Spanner rejects CS queries on regular `ExecuteSql`.

## Performance

Benchmarked on GCP `e2-standard-4` (4 vCPU, 16GB RAM, `asia-northeast1-a`) with production Spanner (`regional-asia-northeast1`, 1000 PU) in the same region — not the emulator. Numbers reflect single-node spanner-etcd; scale further by adding replicas behind a load balancer.

### Direct gRPC benchmark — PENDING_COMMIT_TIMESTAMP revision

Revisions use Spanner's `PENDING_COMMIT_TIMESTAMP()` — each write is fully independent, no shared lock.

| Operation | Clients | ops/sec | Avg latency |
|-----------|---------|---------|-------------|
| PUT 256B | 1 | **86** | 11.7ms |
| PUT 256B | 8 | **379** | 2.6ms |
| PUT 256B | 32 | **673** | 1.5ms |
| PUT 1KB | 8 | 218 | 4.6ms |
| PUT 1KB | 32 | 459 | 2.2ms |
| GET | 1 | 71 | 14.0ms |
| GET | 8 | 597 | 1.7ms |
| GET | 32 | **1,391** | 0.7ms |
| GET | 64 | **1,504** | 0.7ms |

Both reads and writes scale with concurrency. No serialization bottleneck.

**vs. integer counter (previous implementation, same hardware):**

| Clients | Integer counter | PCT | Speedup |
|---------|-----------------|-----|---------|
| PUT ×1 | 24 ops/sec | 86 ops/sec | **3.6×** |
| PUT ×8 | 58 ops/sec | 379 ops/sec | **6.5×** |
| PUT ×32 | 44 ops/sec | 673 ops/sec | **15×** |

### Kubernetes workload — kubeadm v1.31 (100 pods)

| Metric | Value |
|--------|-------|
| 50 Deployments created (parallel) | 3.75s |
| 100 pods Ready | 44s |
| KV/Range avg latency | ~22ms |

Write latency is bounded by one Spanner RW transaction with `PENDING_COMMIT_TIMESTAMP()` — no counter increment, no contention.

Watch latency with Change Streams is ~10–50ms. The poll fallback (1s) is used automatically on the Spanner emulator.

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
| `--auth-users` | `ETCD_AUTH_USERS` | — | `user1:pass1,user2:pass2` — empty = auth disabled |
| `--auth-token-ttl` | — | `5m` | Token lifetime. Clients auto-re-authenticate on expiry. |

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

### 3. As Kubernetes etcd backend (kubeadm)

Tested with **real Kubernetes v1.31 (kubeadm)** — full control plane including
kube-apiserver, kube-controller-manager, kube-scheduler, CoreDNS, Flannel CNI.

```bash
# kubeadm-config.yaml
apiVersion: kubeadm.k8s.io/v1beta3
kind: ClusterConfiguration
kubernetesVersion: v1.31.14
etcd:
  external:
    endpoints:
      - http://127.0.0.1:2379   # spanner-etcd running as systemd service
networking:
  podSubnet: 10.244.0.0/16

# Init
kubeadm init \
  --config=kubeadm-config.yaml \
  --ignore-preflight-errors=ExternalEtcdVersion
```

> **Note**: spanner-etcd must be started before `kubeadm init`. The initial
> revision is seeded to 1 (not 0) — Kubernetes API server rejects revision=0
> as `illegal resource version from storage: 0`.

### 4. As Kubernetes etcd backend (k3s)

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

## Multi-Region Setup

Spanner supports multi-region instance configurations out of the box — no changes to spanner-etcd are needed. Just choose the right instance config when creating your Spanner instance.

### Instance configs

| Config | Coverage | RPO | Write latency | Use case |
|--------|----------|-----|---------------|---------|
| `regional-us-central1` | Single region | 0 | ~5ms | Dev / single-zone HA |
| `nam4` | US East + West | 0 (synchronous) | ~10ms | US multi-region |
| `eur3` | EU West + North | 0 (synchronous) | ~10ms | EU multi-region |
| `asia1` | Asia East + West | 0 (synchronous) | ~10ms | Asia multi-region |
| `nam-eur-asia1` | US + EU + Asia | 0 (synchronous) | ~30ms | Global |

Multi-region configs replicate writes synchronously across regions before acknowledging. `RPO=0` means **zero data loss** even if an entire region fails — stronger than etcd's single-region Raft.

### Creating a multi-region instance

```bash
# US multi-region (East + West)
gcloud spanner instances create k8s-etcd \
  --config=nam4 \
  --description="spanner-etcd multi-region" \
  --processing-units=1000 \
  --project=MY_PROJECT

gcloud spanner databases create etcd \
  --instance=k8s-etcd \
  --project=MY_PROJECT
```

Then point spanner-etcd at it:

```bash
spanner-etcd \
  --spanner-database=projects/MY_PROJECT/instances/k8s-etcd/databases/etcd
```

### Horizontal scaling with multi-region

```
us-east1                                  us-west1
┌──────────────────────────┐             ┌──────────────────────────┐
│  spanner-etcd replica 1  │             │  spanner-etcd replica 2  │
│  spanner-etcd replica 2  │             │  spanner-etcd replica 3  │
└────────────┬─────────────┘             └─────────────┬────────────┘
             │                                         │
             └──────────────┬──────────────────────────┘
                            │
                   Google Cloud Spanner
                   (nam4 multi-region)
                   synchronous replication
```

Clients in the east connect to east replicas (~1ms), clients in the west connect to west replicas (~1ms). All replicas share the same Spanner database. Watch events via Change Streams are delivered per replica with ~10–50ms latency.

### Processing units

Spanner is billed by processing units (PUs). For Kubernetes etcd workloads:

| Cluster size | Recommended PUs | Notes |
|---|---|---|
| < 100 nodes | 100 PUs | Minimum for regional |
| 100–1000 nodes | 500–1000 PUs | |
| 1000–10000 nodes | 1000–3000 PUs | |
| 65000 nodes (GKE scale) | 5000+ PUs | Google's internal estimate |

Multi-region configs have a minimum of 1000 PUs.

## Monitoring

`spanner-etcd` exposes standard gRPC health check on the same port:

```bash
grpc_health_probe -addr=localhost:2379 -tls \
  -tls-ca-cert=ca.crt -tls-client-cert=client.crt -tls-client-key=client.key
```

Slow RPCs (>500ms) are logged at `info` level with method name and elapsed time. Set `--log-level=debug` to log all RPCs.

### Structured logging (Google Cloud Logging)

Logs are emitted as JSON with Google Cloud Logging field names:

```json
{"severity":"INFO","time":"2026-06-04T10:00:00Z","caller":"server/main.go:54","message":"spanner-etcd listening","addr":"0.0.0.0:2379"}
{"severity":"WARNING","time":"...","message":"compact rows delete failed","count":0}
{"severity":"ERROR","time":"...","message":"partition read error, retrying"}
```

`severity` maps to GCP severity levels (DEBUG / INFO / WARNING / ERROR / CRITICAL), enabling filtering and alerting in Cloud Console and Cloud Monitoring without a custom log parser.

## Known limitations and design trade-offs

### Write throughput

Writes use `PENDING_COMMIT_TIMESTAMP()` as the revision — each transaction is fully independent. Write throughput scales with Spanner processing units and replica count. At 1000 PU: **~670 concurrent writes/sec** at ×32 concurrency.

Reads use `Single()` strong reads — fully parallel, no coordination.

### Watch fan-out

With 10,000 concurrent watchers and 1,000 writes/sec, each write triggers fan-out to matching subscribers in all replicas. The current implementation dispatches synchronously in `dispatchEvents`. Under extreme fan-out this becomes a goroutine scheduling bottleneck. Mitigation: increase replica count — each replica handles a subset of Watch connections independently.

### Not implemented

Auth (UserAdd/RoleAdd/Authenticate), Defrag, Snapshot — these are not needed for standard Kubernetes API server operation but may be required for some etcd-compatible tools.

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
- [x] **Kubernetes v1.31 (kubeadm) running with spanner-etcd as etcd backend** — full control plane, pods, CoreDNS, CNI
- [x] Integration tests against Spanner emulator — 38 tests, full stack coverage
- [ ] etcd auth passthrough
- [x] Multi-region Spanner configuration and scaling guidance
- [ ] Spanner Change Streams for emulator (currently uses poll fallback)

## Testing

Tests run against the Spanner emulator and cover the full stack: store operations, Watch delivery, Lease TTL, and the gRPC server layer (38 tests total).

```bash
# Start Spanner emulator
docker run -d -p 9010:9010 -p 9020:9020 \
  gcr.io/cloud-spanner-emulator/emulator

# Run all tests
SPANNER_EMULATOR_HOST=localhost:9010 go test ./...

# Run with verbose output
SPANNER_EMULATOR_HOST=localhost:9010 go test ./... -v -timeout=120s

# Run a specific package
SPANNER_EMULATOR_HOST=localhost:9010 go test ./pkg/store/... -v
SPANNER_EMULATOR_HOST=localhost:9010 go test ./pkg/server/... -v
```

Tests skip gracefully when the emulator is not running. Each test creates its own isolated Spanner database and cleans up after itself.

| Package | Tests | Coverage |
|---------|-------|---------|
| `pkg/store` | 23 | Create, Get (incl. historical), Update, Delete, List, Count, After, Compact, revision monotonicity, OldValue |
| `pkg/store` (Watch) | 5 | Live events, DELETE type, replay from rev=N, prefix filter, OldValue/PrevKv |
| `pkg/store` (Lease) | 4 | Grant/Revoke, natural TTL expiry, Keepalive |
| `pkg/server` (gRPC) | 9 | Put/Get, Update, Delete, DeleteRange, prefix scan, historical Get, Txn CAS, Compact, Watch |

## Development

```bash
# Build
go mod vendor
go build -o spanner-etcd ./cmd/server/

# Run with race detector
SPANNER_EMULATOR_HOST=localhost:9010 \
go run -race ./cmd/server/ \
  --spanner-database=projects/test/instances/test/databases/test \
  --log-level=debug
```

## License

Apache 2.0
