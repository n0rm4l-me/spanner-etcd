# Configuration

## Server Flags

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
| `--auto-compact-interval` | — | `5m` | Background compaction interval; `0` or `off` = disabled |
| `--auto-compact-age` | — | `5m` | Keep this much history for Watch replay |
| `--log-level` | `LOG_LEVEL` | `info` | `debug` / `info` / `warn` / `error` |
| `--spanner-native-metrics` | — | `false` | Enable Spanner client-side metrics (requires `roles/monitoring.metricWriter`) |
| `--spanner-read-location` | — | — | GCP region for directed reads (e.g. `us-east1`). Use with multi-region Spanner to serve reads from the local replica instead of the leader. Writes always go to the leader regardless. |

## Workload Identity Federation (WIF)

spanner-etcd authenticates to Spanner via WIF — no service account key files needed.

```bash
# Bind the Kubernetes ServiceAccount to a GCP service account:
gcloud iam service-accounts add-iam-policy-binding \
  spanner-etcd@MY_PROJECT.iam.gserviceaccount.com \
  --role=roles/iam.workloadIdentityUser \
  --member="principal://iam.googleapis.com/projects/PROJECT_NUMBER/locations/global/workloadIdentityPools/MY_PROJECT.svc.id.goog/subject/ns/NAMESPACE/sa/spanner-etcd"

# Grant Spanner access:
gcloud projects add-iam-policy-binding MY_PROJECT \
  --member="serviceAccount:spanner-etcd@MY_PROJECT.iam.gserviceaccount.com" \
  --role="roles/spanner.databaseUser"
```

## Helm Values

Key values in `helm/spanner-etcd/values.yaml`:

```yaml
spannerDatabase: "projects/P/instances/I/databases/D"

replicaCount: 3

auth:
  users: "root:secret"          # inline credentials
  existingSecret: ""            # or use an existing Secret
  tokenTTL: "5m"

terminationGracePeriod:
  preStopSleepSeconds: 15       # endpoint drain time
  seconds: 60                   # total pod termination budget

autoscaling:
  enabled: false
  minReplicas: 3
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70

podDisruptionBudget:
  enabled: true
  minAvailable: 2

podMonitoring:
  enabled: true   # GKE Managed Prometheus
```
