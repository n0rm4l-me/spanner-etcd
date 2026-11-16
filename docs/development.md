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
make docker VERSION=0.4.0

# Build + push to registry
make release VERSION=0.4.0
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

78 integration tests against the Spanner emulator.

| Package | Tests | What's covered |
|---------|-------|----------------|
| `pkg/store` | 37 | Create/Get/Update/Delete/List/Count/After/Compact/Watch/Lease/Lease+Watch/AutoCompact/ErrCompacted |
| `pkg/server` | 38 + 3 auth | Full gRPC stack, Txn atomic/non-atomic/concurrent, Watch cancel/fanout/replay-pagination, graceful shutdown, auth token expiry, LeaseTimeToLive, IgnoreValue/IgnoreLease, range delete boundary |

## CI

GitHub Actions runs on every push to `main` and every PR:

- `test` job: `go vet`, `govulncheck` (fails on a vulnerability the code actually reaches), all integration tests against the Spanner emulator with the race detector, and a check that no test was skipped
- `bench` job (main only): benchmarks with results in Job Summary + artifact

The release workflow runs the same `test` job first (`workflow_call`) and publishes the image and binaries only if it passes.

Actions are pinned by commit SHA (the `# vX.Y.Z` comment names the version) and Dependabot proposes updates weekly. To run the race detector locally on macOS, `CGO_ENABLED=1` needs a working SDK; if linking fails with `unknown architecture` errors from the Command Line Tools SDK, point `SDKROOT` at an older one, for example `SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk`.

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
