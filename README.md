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
    ┌─────────────────────────────────┐
    │  KVServer     WatchServer       │
    │  LeaseServer  ClusterServer     │
    │  MaintenanceServer              │
    │           │                     │
    │      SpannerStore               │
    │   ┌───────────────────────┐     │
    │   │  Write: RW txn        │     │
    │   │  bump rev + insert kv │     │
    │   │                       │     │
    │   │  Watch: poll loop     │     │
    │   │  + fan-out broadcaster│     │
    │   │                       │     │
    │   │  Lease: TTL goroutine │     │
    │   └───────────────────────┘     │
    └──────────────┬──────────────────┘
                   │  Spanner gRPC
                   ▼
         Google Cloud Spanner
```

Multiple `spanner-etcd` replicas can run concurrently — all state lives in Spanner. No consensus, no leader election between replicas.

## Implemented etcd v3 API

| Service | Method | Status | Notes |
|---------|--------|--------|-------|
| **KV** | Range (Get/List) | ✅ | Single key, prefix, range, historical (rev=N), count-only |
| **KV** | Put | ✅ | Create + unconditional update |
| **KV** | DeleteRange | ✅ | Via Txn (Kubernetes-style) |
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
```

### Design decisions

**`id` vs `rev`**: The physical primary key (`id`) uses `bit_reversed_positive` sequence to distribute writes across Spanner splits and avoid hot spots. The logical revision (`rev`) is a simple monotonic integer from `kv_rev`. etcd clients see `rev` as `ModRevision`.

**Atomic revision bump**: Every write does `UPDATE kv_rev SET rev = rev + 1 WHERE id = 1 THEN RETURN rev` — a single DML statement that both increments and returns the new value, avoiding the two-RPC read-modify-write pattern.

**Append-only log**: Like etcd, we never update rows in `kv`. Each write appends a new row. Compaction physically deletes old rows asynchronously.

## Performance

| Operation | Latency (Spanner prod) | Latency (emulator) |
|-----------|----------------------|-------------------|
| Get (single key) | ~5ms | ~20ms |
| Put (create) | ~10ms | ~100ms |
| List (prefix, 100 keys) | ~10ms | ~30ms |
| Watch event delivery | ~1s (poll interval) | ~1s |

Write latency is bounded by Spanner RW transaction: one DML `UPDATE kv_rev THEN RETURN` + one `INSERT INTO kv`. Both happen in a single Spanner transaction.

Watch uses 1-second polling by default. For lower latency, implement [Spanner Change Streams](https://cloud.google.com/spanner/docs/change-streams) (not yet implemented).

## Installation

### Prerequisites

- Go 1.21+
- Google Cloud project with Spanner enabled
- A Spanner instance and database

```bash
go install github.com/paas/spanner-etcd/cmd/server@latest
```

Or build from source:

```bash
git clone https://github.com/paas/spanner-etcd
cd spanner-etcd
go build -o spanner-etcd ./cmd/server/
```

## Configuration

| Flag | Env | Default | Description |
|------|-----|---------|-------------|
| `--spanner-database` | `SPANNER_DATABASE` | — | **Required.** Full Spanner DB path: `projects/P/instances/I/databases/D` |
| `--listen-address` | `LISTEN_ADDR` | `0.0.0.0:2379` | gRPC listen address |
| `--tls-cert` | `TLS_CERT` | — | Server TLS certificate file |
| `--tls-key` | `TLS_KEY` | — | Server TLS private key file |
| `--tls-ca` | `TLS_CA` | — | CA certificate for mTLS client verification |
| `--log-level` | `LOG_LEVEL` | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `--peer-urls` | `PEER_URLS` | — | Peer URLs advertised in MemberList (comma-separated) |

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

All replicas are stateless — no coordination needed. Spanner guarantees external consistency (linearizability) across all replicas. Watch events are delivered independently per replica via the poll loop; each replica polls Spanner every second.

## Monitoring

`spanner-etcd` exposes standard gRPC health check on the same port:

```bash
grpc_health_probe -addr=localhost:2379 -tls \
  -tls-ca-cert=ca.crt -tls-client-cert=client.crt -tls-client-key=client.key
```

Slow RPCs (>500ms) are logged at `info` level with method name and elapsed time. Set `--log-level=debug` to log all RPCs.

## Limitations vs etcd

| Feature | etcd | spanner-etcd |
|---------|------|-------------|
| Watch latency | <10ms | ~1s (poll-based) |
| Lease keepalive | Streaming | Streaming ✅ |
| DeleteRange (bare) | ✅ | Via Txn only |
| Defrag / Snapshot | ✅ | Not implemented |
| Auth (RBAC) | ✅ | Not implemented |
| Watch latency <100ms | ✅ | Requires Change Streams |

### Roadmap

- [ ] Spanner Change Streams for sub-second Watch latency
- [ ] Metrics (Prometheus endpoint)
- [ ] Helm chart for Kubernetes deployment
- [ ] etcd auth passthrough
- [ ] Multi-region Spanner configuration examples

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
