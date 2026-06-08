# Deployment

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

Tested with **Kubernetes v1.33.12 (kubeadm)** — full control plane, CoreDNS, Flannel CNI, Deployments, pods — all working. 24h soak test passed with zero errors.

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

### Processing Units

| Cluster size | Recommended PUs |
|---|---|
| < 100 nodes | 100 PUs |
| 100–1000 nodes | 500–1000 PUs |
| 1000–10000 nodes | 1000–3000 PUs |
| 65000 nodes (GKE scale) | 5000+ PUs |

## Monitoring

`/metrics` (Prometheus) and `/healthz` are served on `--metrics-addr` (default `:2381`).

| Metric | Description |
|--------|-------------|
| `spanner_etcd_active_watches` | Active Watch subscriptions |
| `spanner_etcd_change_stream_active` | 1 = CS delivering events; 0 = poll fallback |
| `spanner_etcd_change_stream_partitions_active` | CS partitions being read |
| `spanner_etcd_current_revision` | Latest revision (UnixNano) |
| `spanner_etcd_rpc_duration_seconds` | gRPC latency histogram by method |
| `spanner_etcd_active_leases` | Active TTL leases |
| `spanner_etcd_compacted_rows_total` | Rows deleted by compaction |
| `spanner_etcd_compaction_duration_seconds` | Compaction run duration |

**GKE Managed Prometheus**: `PodMonitoring` is included in the Helm chart (`podMonitoring.enabled: true`).

### Structured logging (Google Cloud Logging)

```json
{"severity":"INFO","time":"2026-06-05T10:00:00Z","caller":"server/main.go:54","message":"spanner-etcd listening","addr":"0.0.0.0:2379"}
{"severity":"INFO","time":"...","message":"authenticated","user":"root","client":"10.244.0.15:52341"}
{"severity":"WARNING","time":"...","message":"partition read error, retrying"}
```
