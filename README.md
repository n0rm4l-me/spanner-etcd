# spanner-etcd

[![test](https://github.com/n0rm4l-me/spanner-etcd/actions/workflows/test.yml/badge.svg)](https://github.com/n0rm4l-me/spanner-etcd/actions/workflows/test.yml)
[![Go](https://img.shields.io/badge/Go-1.21+-00ADD8?logo=go)](https://go.dev)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Kubernetes](https://img.shields.io/badge/Kubernetes-v1.33-326CE5?logo=kubernetes)](https://kubernetes.io)

**A production-grade, drop-in etcd replacement backed by Google Cloud Spanner.**

Implements the complete etcd v3 KV/Watch/Lease/Auth gRPC API. Deploy spanner-etcd in place of etcd and gain unlimited horizontal scale, native multi-region replication, and 99.999% availability — with zero etcd cluster management overhead.

```
# Before
--etcd-servers=https://etcd-0:2379,https://etcd-1:2379,https://etcd-2:2379

# After
--etcd-servers=http://spanner-etcd:2379
```

## Highlights

| | etcd | spanner-etcd |
|---|---|---|
| Storage | Raft log on local disk | Google Cloud Spanner |
| Horizontal scale | Limited (3–5 members) | Unlimited stateless replicas |
| Cross-region | Manual federation | Native (Spanner multi-region) |
| Durability | Single-region by default | 99.999% SLA |
| Operations | Cluster management, backups | Fully managed by Google |
| Write throughput (×32) | baseline | **15× faster** (PENDING_COMMIT_TIMESTAMP) |
| Watch latency | ~1ms (local) | **10–50ms** (Change Streams) |

## Why

Standard etcd is a single-region, single-cluster system. Under sustained load it becomes a write bottleneck — a global counter serializes every mutation. The GKE team replaced etcd with a Spanner-backed system to scale Kubernetes clusters to [65,000+ nodes](https://cloud.google.com/blog/products/containers-kubernetes/gke-65k-nodes-and-counting).

spanner-etcd is an open implementation of the same idea:

- **PENDING_COMMIT_TIMESTAMP** as revision — no shared counter, no write lock, each transaction is fully independent
- **Spanner Change Streams** for Watch — push-based delivery at ~10–50ms latency
- **Atomic Txn** — compare+ops in a single Spanner ReadWriteTransaction
- **Stateless replicas** — all state in Spanner, scale out by adding pods

## Quick Start

```bash
# Start Spanner emulator
docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# Create instance and database
curl -s -X POST http://localhost:9020/v1/projects/my-project/instances \
  -d '{"instanceId":"my-instance","instance":{"config":"emulator-config","displayName":"dev","nodeCount":1}}'
curl -s -X POST http://localhost:9020/v1/projects/my-project/instances/my-instance/databases \
  -d '{"createStatement":"CREATE DATABASE `my-db`"}'

# Run spanner-etcd
SPANNER_EMULATOR_HOST=localhost:9010 spanner-etcd \
  --spanner-database=projects/my-project/instances/my-instance/databases/my-db

# Test
etcdctl put /hello world && etcdctl get /hello
```

## Documentation

| Doc | Contents |
|---|---|
| [Architecture](docs/architecture.md) | Design decisions, Spanner schema, etcd API coverage, known limitations |
| [Deployment](docs/deployment.md) | GKE/Helm, kubeadm, TLS, migration, monitoring |
| [Configuration](docs/configuration.md) | All flags, Workload Identity Federation, Helm values |
| [Performance](docs/performance.md) | Benchmarks, production validation, soak test results |
| [Development](docs/development.md) | Build, test, CI, Makefile targets |

## Status

| Component | Status |
|---|---|
| etcd v3 KV/Watch/Lease/Auth API | ✅ Complete |
| Atomic Txn (single Spanner RWT) | ✅ Complete |
| Spanner Change Streams | ✅ Production Spanner |
| Background auto-compaction | ✅ Complete |
| Prometheus metrics + GKE PodMonitoring | ✅ Complete |
| Helm chart (WIF, PDB, HPA) | ✅ Complete |
| 78 integration tests | ✅ Passing |
| Kubernetes v1.33.12 (kubeadm) | ✅ Validated |
| 24h soak test (Kubernetes v1.33) | ✅ Complete |
| TLS / mTLS in production | ⏳ Planned |
| Multi-replica HA validation | ⏳ Planned |

## License

Apache 2.0
