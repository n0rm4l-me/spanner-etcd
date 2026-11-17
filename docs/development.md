# Development

## Prerequisites

- Go 1.26.11 or newer (see `go` in go.mod; an older local toolchain downloads the right one automatically)
- Google Cloud project with Spanner enabled
- [Spanner emulator](https://cloud.google.com/spanner/docs/emulator) for local testing

## Build

```bash
# Build local binary
make build

# Build Docker image (linux/amd64) — runs 'make vendor' automatically
make docker VERSION=v0.3.1

# Build + push to registry
make release VERSION=v0.3.1
```

> **Registry**: `make docker`, `make push` and `make release` need `IMAGE_REPO`, which defaults to the placeholder `YOUR_REGISTRY/spanner-etcd` and makes them stop with a message. Put your registry in an untracked `Makefile.local` (`IMAGE_REPO ?= your-registry/path/spanner-etcd`) or pass `IMAGE_REPO=...` on the command line. `make push` logs in to the host part of `IMAGE_REPO` with a `gcloud` access token, so it expects a Google Artifact Registry host.

> **Note on vendor**: `vendor/` is not committed. `make docker` runs `make vendor` automatically.
> In corporate networks with custom CA certificates, `go mod download` inside Docker
> fails — vendoring locally avoids this.

## Testing

```bash
# Start Spanner emulator
docker run -d -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator

# Run all tests
make test

# Run with verbose output
make test-v

# Run with race detector
SPANNER_EMULATOR_HOST=localhost:9010 \
go run -race ./cmd/server/ \
  --spanner-database=projects/test/instances/test/databases/test \
  --log-level=debug
```

### Test coverage

128 tests, almost all of them integration tests against the Spanner emulator (the count is of `Test` functions in `pkg/`; refresh it with `grep -h '^func Test' pkg/*/*_test.go | wc -l` whenever tests are added or removed).

| Package | Tests | What's covered |
|---------|-------|----------------|
| `pkg/store` | 73 | Create/Put/Get/Update/Delete/List/Count/After/Compact/Watch/Lease/Lease+Watch/AutoCompact/ErrCompacted, Watch exactly-once and in-order delivery, exact ranges (`PrefixEnd`, non-ASCII, historical, single-key watch, continuation keys ending in NUL), Change Stream events as hints only, the Change Stream attaching on a fresh database (start retry), replicas applying the schema together on an empty database, remote-write latency, multi-replica leases (keepalive on another replica, restart, owner death), legacy `kv_lease` rows, schema upgrade, writes to dead leases (Put/Create/Update/Txn, race with Revoke), compaction (deleted keys removed completely, newest tombstone kept so the revision never moves back, compaction lease, stale cursor cleanup) |
| `pkg/server` | 51 + 3 auth | Full gRPC stack, exact ranges (sibling prefixes, wildcards, limit/More paging, sorting, DeleteRange, Watch), Put (concurrent, PrevKv, lease-not-found and re-grant), `Server.Addr` against `Serve` (race detector), Txn atomic/non-atomic/concurrent, Watch cancel/fanout/replay-pagination, graceful shutdown, auth token expiry, LeaseTimeToLive, IgnoreValue/IgnoreLease, range delete boundary |

Unit tests that need no Spanner: `cmd/server` (flag and environment parsing and validation), `pkg/schema` (which schema errors are retried) and the pure `PrefixEnd`/`InRange` tests in `pkg/store`.

The emulator tests skip silently when the emulator is not reachable, so a green
run with no emulator proves nothing. Check for skips with
`go test ./pkg/... -v | grep -c SKIP` (expect 0).

### Running against a real Spanner instance

The emulator differs from production Spanner in SQL dialect details, and its
Change Stream is not a substitute for the real one (for example, the hint
latency measured on a real instance does not carry over). The `pkg/store` and
`pkg/server` tests can run against a real instance:

```bash
SPANNER_ETCD_TEST_INSTANCE=projects/PROJECT/instances/INSTANCE \
  go test ./pkg/store/ ./pkg/server/ -run 'TestLease|TestRange_' -timeout 20m -v
```

This uses application default credentials (`gcloud auth application-default login`)
and creates and drops one database per test, which takes a minute or two each, so
select tests with `-run`. Use a throwaway instance (100 PU is enough) and delete
it when you are done: it is billed while it exists.

## CI

GitHub Actions runs on every push to `main` and every PR:

- `test` job: `go vet`, `govulncheck` (fails on a vulnerability the code actually reaches), all integration tests against the Spanner emulator with the race detector, and a check that no test was skipped
- `bench` job (main only): benchmarks with results in Job Summary + artifact

The release workflow runs the same `test` job first (`workflow_call`) and publishes the image and binaries only if it passes.

Actions are pinned by commit SHA (the `# vX.Y.Z` comment names the version) and Dependabot proposes updates weekly (`.github/dependabot.yml`). Minor and patch updates of one ecosystem arrive as a single pull request, major updates stay separate, the etcd api and client modules always move together, a new release waits a week before it is proposed, and the Go image stays on the Go version in `go.mod`. When reviewing: a green grouped minor/patch PR can be merged as is; look at each major update, especially the actions used only by the release workflow (login, build-push, gh-release), which no pull request exercises, so the first release after merging is their first run. To run the race detector locally on macOS, `CGO_ENABLED=1` needs a working SDK; if linking fails with `unknown architecture` errors from the Command Line Tools SDK, point `SDKROOT` at an older one, for example `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`.

## Releasing

1. Move the entries under `## Unreleased` in `CHANGELOG.md` to a new `## vX.Y.Z - date` section, and bump `version` and `appVersion` (with the `v` prefix) in `helm/spanner-etcd/Chart.yaml`. Both go through a pull request like any other change.
2. Optionally try a release candidate first: push a tag with a suffix, for example `v0.3.1-rc.1`. The release workflow runs the whole `test` job, then builds and publishes the image for `linux/amd64` and `linux/arm64`, the binaries and a GitHub release. A suffixed tag is treated as a pre-release: its image is tagged with the tag only (`:latest` is not moved) and the GitHub release is marked as a pre-release.
3. Push the final tag, for example `v0.3.1`. The same workflow publishes it, moves `:latest` and marks the GitHub release as the latest one. The release notes are the section of the CHANGELOG whose heading starts with the tag.

Nothing is published unless the `test` job passes. The release workflow runs only on tags, so no pull request exercises it; a release candidate is how to check changes to it.

## Makefile targets

| Target | Description |
|--------|-------------|
| `make build` | Build local binary (host OS/arch) |
| `make build-linux` | Build `spanner-etcd-linux-amd64` for VM/server deployment |
| `make vendor` | `go mod tidy && go mod vendor` |
| `make docker` | Build linux/amd64 Docker image (runs vendor first) |
| `make push` | Push image to registry |
| `make release` | docker + push |
| `make test` | Run integration tests against emulator |
| `make test-v` | Same with verbose output |
| `make lint` | `go vet ./...` |
| `make clean` | Remove build artifacts |
