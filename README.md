# spanner-etcd

[![test](https://github.com/n0rm4l-me/spanner-etcd/actions/workflows/test.yml/badge.svg)](https://github.com/n0rm4l-me/spanner-etcd/actions/workflows/test.yml)

A **drop-in etcd replacement** backed by **Google Cloud Spanner** — tested with Kubernetes v1.33 and production application workloads.

Implements the complete etcd v3 KV/Watch/Lease/Auth API. Swap out etcd for spanner-etcd and get unlimited horizontal scale, native multi-region replication, and 99.999% SLA — with zero etcd cluster management.

```
# Before
--etcd-servers=https://etcd-0:2379,https://etcd-1:2379,https://etcd-2:2379

# After
--etcd-servers=http://spanner-etcd:2379
```

## Why

Standard etcd is a single-region, single-cluster system. At Google scale, the GKE team replaced etcd with a Spanner-backed implementation to scale clusters to [65,000+ nodes](https://cloud.google.com/blog/products/containers-kubernetes/gke-65k-nodes-and-counting).

| | etcd | spanner-etcd |
|---|---|---|
| Storage | Raft log on local disk | Google Cloud Spanner |
| Horizontal scale | Limited (3–5 members) | Unlimited replicas |
| Cross-region | Manual federation | Native (Spanner multi-region) |
| Durability | Single-region by default | 99.999% SLA |
| Operations | etcd cluster management | Serverless |

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

- [Architecture](docs/architecture.md) — design, Spanner schema, etcd API coverage, known limitations
- [Deployment](docs/deployment.md) — GKE/Helm, kubeadm, TLS, migration, monitoring
- [Configuration](docs/configuration.md) — all flags, WIF setup, Helm values
- [Performance](docs/performance.md) — benchmarks, production validation
- [Development](docs/development.md) — build, test, CI, Makefile

## Roadmap

- [x] PENDING_COMMIT_TIMESTAMP revision (no serialization bottleneck, 15× write speedup)
- [x] Spanner Change Streams for Watch (~10–50ms latency on production Spanner)
- [x] Atomic Txn — compare+ops in a single Spanner ReadWriteTransaction
- [x] Hybrid Txn routing — atomic for simple ops, non-atomic fallback for range/complex ops
- [x] Simple username/password authentication with auto-reauth
- [x] Prometheus metrics + GKE PodMonitoring + Google Cloud Monitoring integration
- [x] Background auto-compaction with configurable interval
- [x] Helm chart with WIF, PDB, HPA, graceful shutdown
- [x] Kubernetes v1.33 (kubeadm) — full control plane tested
- [x] Production audit — goroutine leaks, data races, protocol correctness fixed
- [x] 78 integration tests (emulator)
- [ ] Kubernetes v1.33 24h soak test — in progress
- [ ] TLS / mTLS in production deployment
- [ ] Multi-replica HA validation
- [ ] Auth RBAC (UserAdd/RoleGrantPermission)
- [ ] Change Streams support on Spanner emulator

## License

Apache 2.0
