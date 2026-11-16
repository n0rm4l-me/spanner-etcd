# Container registry for `make docker` / `make push`. The real value is not kept
# in the repository: put it in an untracked Makefile.local, for example
#   IMAGE_REPO ?= europe-docker.pkg.dev/MY_PROJECT/MY_REPO/spanner-etcd
# or pass it on the command line: make push IMAGE_REPO=...
-include Makefile.local

IMAGE_REPO    ?= YOUR_REGISTRY/spanner-etcd
REGISTRY_HOST ?= $(firstword $(subst /, ,$(IMAGE_REPO)))
VERSION       ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

.PHONY: build build-linux vendor docker push test lint clean check-registry

## Fail early if the registry is still the placeholder
check-registry:
	@case "$(IMAGE_REPO)" in \
	  YOUR_REGISTRY*) echo "IMAGE_REPO is not set: create Makefile.local or pass IMAGE_REPO=..."; exit 1;; \
	esac

## Build local binary (host OS/arch)
build:
	go build -ldflags="-s -w" -o spanner-etcd ./cmd/server/
	go build -ldflags="-s -w" -o spanner-etcd-migrate ./cmd/migrate/

## Build linux/amd64 binary for deployment to VMs
build-linux:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
	  go build -ldflags="-s -w" -o spanner-etcd-linux-amd64 ./cmd/server/
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 \
	  go build -ldflags="-s -w" -o spanner-etcd-migrate-linux-amd64 ./cmd/migrate/

## Download and vendor dependencies (required before docker)
vendor:
	go mod tidy
	go mod vendor

## Build Docker image for linux/amd64
## Requires: make vendor first (or GOPROXY set)
docker: check-registry vendor
	podman build --platform linux/amd64 \
	  -f Dockerfile \
	  -t $(IMAGE_REPO):$(VERSION) \
	  .

## Push image to registry (re-login if needed)
push: check-registry
	@gcloud auth print-access-token | \
	  podman login $(REGISTRY_HOST) \
	    --username=oauth2accesstoken --password-stdin
	podman push $(IMAGE_REPO):$(VERSION)

## Build and push in one step
release: docker push

## Run tests against Spanner emulator
test:
	SPANNER_EMULATOR_HOST=localhost:9010 \
	  go test ./... -timeout=120s -count=1

## Run tests with verbose output
test-v:
	SPANNER_EMULATOR_HOST=localhost:9010 \
	  go test ./... -v -timeout=120s -count=1

## Run linter
lint:
	golangci-lint run ./...

## Remove built binaries and vendor
clean:
	rm -f spanner-etcd spanner-etcd-migrate
	rm -f spanner-etcd-linux-amd64 spanner-etcd-migrate-linux-amd64
	rm -rf vendor/
