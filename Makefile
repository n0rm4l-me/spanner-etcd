IMAGE_REPO ?= asia-docker.pkg.dev/onepoint-paas-tardis-4492/docker-paas-tardis-asia-1/spanner-etcd
VERSION     ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")

.PHONY: build vendor docker push test lint clean

## Build local binary (host OS/arch)
build:
	go build -ldflags="-s -w" -o spanner-etcd ./cmd/server/
	go build -ldflags="-s -w" -o spanner-etcd-migrate ./cmd/migrate/

## Download and vendor dependencies (required before docker)
vendor:
	go mod tidy
	go mod vendor

## Build Docker image for linux/amd64
## Requires: make vendor first (or GOPROXY set)
docker: vendor
	podman build --platform linux/amd64 \
	  -f Dockerfile \
	  -t $(IMAGE_REPO):$(VERSION) \
	  .

## Push image to registry (re-login if needed)
push:
	@gcloud auth print-access-token | \
	  podman login asia-docker.pkg.dev \
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
	rm -rf vendor/
