# The build stage always runs on the machine doing the build and cross-compiles for
# the requested platform, so a multi-platform build needs no emulation (QEMU).
FROM --platform=$BUILDPLATFORM golang:1.26-alpine AS builder

# Set by buildx/podman from --platform; the defaults keep a plain `docker build` working.
ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

# Copy dependency manifests first for better layer caching.
COPY go.mod go.sum ./

# Copy vendor if it exists (created by `make vendor` or `go mod vendor`).
# In corporate environments with custom CA, run `make vendor` locally first.
# Without vendor, set GOPROXY/GONOSUMDB to an accessible proxy.
COPY vendor/ vendor/

COPY . .

# ENV, not a prefix on one command: both binaries must be built for the target.
ENV CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH
# The Go build cache is about a gigabyte and useless once the binaries exist;
# clearing it in the same RUN keeps this layer to the two binaries.
RUN go build -mod=vendor -ldflags="-s -w" -o /spanner-etcd ./cmd/server/ && \
    go build -mod=vendor -ldflags="-s -w" -o /spanner-etcd-migrate ./cmd/migrate/ && \
    go clean -cache

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /spanner-etcd /spanner-etcd
COPY --from=builder /spanner-etcd-migrate /spanner-etcd-migrate
EXPOSE 2379 2381
ENTRYPOINT ["/spanner-etcd"]
