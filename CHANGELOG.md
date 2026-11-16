# Changelog

## v0.3.0 - 2026-11-16

The first release since v0.2.0. It fixes data-loss and correctness bugs found in a systematic review, makes Watch and leases work correctly with more than one replica, and clears 24 vulnerabilities that the code actually reached. Read the upgrade notes below before rolling out, in particular the one about the schema.

### Highlights

- **No more silent loss.** A bare `Put` no longer reports success for a write that did not happen; `DeleteRange` on a prefix no longer deletes keys outside it; writes that name a dead lease are rejected, as in etcd, instead of creating keys that never expire.
- **Leases work with several replicas.** Their state lives in Spanner, so a keepalive can be served by any replica and survives restarts. This needs a new column, `kv_lease.expires_at`: see the schema note below.
- **Watch delivers each event once, in order, and the Change Stream now works.** Events from other replicas arrive in well under a second instead of up to 30 seconds.
- **Exact ranges, correct paging.** Prefix and range reads, deletes and watches cover exactly the right keys; `More` is set, so the Kubernetes API server's paged lists are complete; sort options work.
- **The table no longer grows without bound.** Compaction removes deleted keys completely, and one replica compacts per round.
- **Security and hardening.** Go 1.26.11 and updated dependencies remove 24 reachable vulnerabilities, one of them a remote panic in the gRPC server. The server refuses to start on an unknown flag or half a TLS configuration. The image is built correctly for arm64.

### Upgrade notes (leases)

- **Schema change: `kv_lease.expires_at`.** A new replica applies `ALTER TABLE kv_lease ADD COLUMN IF NOT EXISTS expires_at TIMESTAMP` at startup. If the runtime service account has no DDL permission (schema managed externally), apply the updated [ddl/schema.sql](ddl/schema.sql) **before** rolling out the new version, otherwise every lease operation fails with a missing-column error.
- **Rolling upgrade.** Old replicas do not persist keepalives. A lease that is kept alive only through old replicas is expired by new replicas at its original grant time plus TTL, so during a rolling upgrade some leases can expire early. Clients re-grant a lost lease (jetcd does), so this is a short blip, but keep the upgrade window short.
- `spanner_etcd_active_leases` is now the cluster-wide count of unexpired leases, refreshed about every 15 seconds, not a per-replica in-memory count.
- Library users: `LeaseManager.Get` was removed and `GetTTL` now takes a context and returns an error.

### Upgrade notes (flags)

- A deployment that passes a flag the server does not know, an invalid value or half of a TLS pair used to start anyway; it will now fail at startup. Check your arguments before upgrading. The Helm chart in this repository passes only flags that are valid.

### Upgrade notes (ranges)

- Behaviour change: reads, deletes and watches over a prefix or range now cover exactly the keys etcd would include. Clients that relied on the old over-inclusion (for example a watch on `/a/` that also saw `/ab/`) will no longer see those keys.
- With a `limit`, `Count` is the number of keys returned, not the size of the whole range (see docs/architecture.md, Range reads).
- `DeleteRange` over a range now fails with the first error instead of returning a partial success.

### Upgrade notes (Watch)

- Previously reported Watch latency of about 30 ms measured a write and a watch on the same replica, which never used the Change Stream. See the note in [docs/performance.md](docs/performance.md). The 24-hour soak result "Change Stream active 100%" came from a gauge set by a timer, not from delivered events.
- `spanner_etcd_watch_events_total{source}` now says which signal triggered the read that delivered the events: `change_stream` for a Change Stream wake-up, `poll` for a local write or the timer. All events come from the same ordered read.
- With the Change Stream working, each replica reads every partition of `kv_changes`, so expect more `READ_kv_changes` traffic than before on instances with many partitions.

### Dependencies and tooling

- Go modules: `google.golang.org/grpc` 1.83.2 to 1.85.0, the Spanner client 1.91.0 to 1.96.1 (checked on the emulator only), etcd `api` and `client` 3.6.11 to 3.7.3 (moved together; the pair builds and passes the whole suite), `prometheus/client_golang` 1.23.2 to 1.25.0.
- GitHub Actions to their current major versions: `checkout` 7, `upload-artifact` 7, `docker/login-action` 4, `docker/build-push-action` 7, `softprops/action-gh-release` 3 (all still pinned by commit SHA). The release workflow runs only on tags, so the first release is the first run of its updated actions.
- Dependabot now groups minor and patch updates into one pull request per ecosystem, keeps the etcd modules together, waits a week before proposing a new release and no longer proposes a newer Go image than `go.mod` declares.

### Fixed

- **Typos and bad values in server flags were ignored silently.** The hand-written flag parser skipped anything it did not recognise, so `--tls-sert=...` started the server without TLS, `--tls-cert` without `--tls-key` quietly ran in plaintext, and an unparsable `--auth-token-ttl` kept the default without a word. The server now uses the standard `flag` package and refuses to start (exit code 2) on an unknown flag, a stray argument, an unparsable or negative duration, an inconsistent set of TLS flags, or an empty `--spanner-read-location`. `--flag value` now works as well as `--flag=value`, and `--help` lists every flag.
- **Ranges were approximated with `LIKE 'prefix%'`, so `DeleteRange` could delete keys outside the range.** A prefix was turned into a LIKE pattern with its last character dropped: `DeleteRange` on `/del/a/` also deleted `/del/ab/3` and `/del/a0` (four keys deleted where two were asked for). The same approximation made `Range` return keys beyond `range_end` (an explicit range `[b, d)` included `d` and everything after it), treat `_` and `%` in keys as wildcards, and made Watch on `/wp/a/` deliver events for `/wp/ab/...` and `/wp/a0`. Ranges are now exact: `[key, range_end)` in byte order, with `range_end` `\x00` meaning "from key onward", for Range, Count, DeleteRange, Txn range operations and Watch alike. Keys and range ends that are not valid UTF-8 are rejected with `InvalidArgument`.
- **Range never set `More`.** A read with a limit therefore looked complete after its first page: the Kubernetes API server, which pages its lists through `More`, and `etcdctl get --limit` would silently see a truncated result on any list longer than the limit. `More` is now exact.
- **Range ignored the sort options.** `WithLastKey()` returned the first key, and `WithFirstKey()`, `WithSort(...)` and the other sort modes did nothing. Sorting by key, create revision, modification revision and value, ascending and descending, now works; sorting by version returns `Unimplemented` (see docs/architecture.md, Range reads).
- **`DeleteRange` reported success after failed deletes.** It skipped keys whose delete failed and returned the count of the rest. It now returns the first error.
- **Deleted keys were never fully removed, so the table grew without bound.** Compaction deleted superseded values but kept the tombstone of every deleted key forever, and every replica ran its own full-table compaction pass every interval. A store with a steady churn of short-lived keys (Kubernetes events, jetcd registrations that expire) therefore gained a row per key it ever held, and each compaction pass got slower. Compaction now also deletes a tombstone once the key has no other row left and the deletion is at or below the compaction revision, which is what etcd does. It never removes a tombstone that still has an older value behind it (that would bring the key back) or the newest row in the table (that would move the current revision backwards). The background compactor takes a short lease first, so one replica per interval does the work. Change Stream cursors that have not been written for a day (they belong to replicas that no longer exist) are deleted by the same loop.
- **Watch delivered every event two or three times and sometimes out of order.** Subscribers were fed by two independent paths, the poll loop and the Change Stream reader, with no deduplication and no common ordering, and the poll loop itself could re-read events because it advanced to a "current revision" taken in a different snapshot from the events query. Measured against a real Spanner instance: 20 committed revisions produced 41 deliveries, and a write made through another replica could arrive after a later write made through the watched one. A PUT arriving after a newer DELETE leaves a client with the wrong final state. There is now a single delivery path: one ordered read of the table (`rev` above the last delivered revision) that advances to the last event it delivered. The Change Stream no longer delivers events itself; it only tells that read to run now. The same measurement now gives 20 deliveries for 20 revisions, in order. A batch boundary no longer splits a multi-key transaction (all its keys share one revision).
- **The Change Stream reader followed only one of the stream's initial partitions.** The discovery query stopped after the first record containing child partitions, so every other partition was never read. A stream normally has several, and writes land in whichever partition owns the key (the `id` sequence is bit-reversed, so in the instance tested all writes went to the unread one). As a result no event ever arrived through the Change Stream, the "change stream active" gauge notwithstanding, and an event from a write made through another replica waited for the poll timer, up to 30 seconds. The query is now bounded and reads every partition. Against a real instance such an event now reaches the subscriber in about 0.1 to 1.5 s including the write (see the next item for why it varies).
- **The poll timer slowed to 30 s ten seconds after startup whether or not the Change Stream worked.** On the emulator, or on any instance where the stream is unavailable, remote events were delayed by up to 30 s after that. The Change Stream's own latency turned out to be unpredictable on a real instance: a hint arrives either about 15 ms after the commit or one 2 s heartbeat later, varying from run to run, so the timer must not depend on it. The timer now stays at 1 s always and bounds delivery latency; the Change Stream only makes it faster when it is in its fast mode. `spanner_etcd_change_stream_active` now reports whether the stream delivered a record in the last 10 s, instead of being set by a timer.
- **Keys could be attached to a lease that no longer existed, and then lived forever.** `Put`, `Txn` puts and the internal create/update paths accepted any lease id without checking it, so a write that raced a lease expiry, or reused an id after revoke, left a key that nothing would ever delete (a service-registry entry that never goes away). Such writes are now rejected inside the write transaction with etcd's error (`NotFound`, `etcdserver: requested lease not found`), which etcd clients recognise as the signal to grant a new lease. A `Txn` with one dead lease fails as a whole and applies nothing. Revoke and expiry now scan for the lease's keys a second time after removing the lease row, so a Put that validated the lease just before it was removed is cleaned up as well. Behaviour change for clients: a write that names an unknown or expired lease now fails where it used to succeed silently.
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
