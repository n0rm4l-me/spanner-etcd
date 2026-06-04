FROM golang:1.26-alpine AS builder
WORKDIR /src
COPY go.mod go.sum ./
# vendor/ is created locally via `go mod vendor` before building.
# This avoids network access during the Docker build, which is required
# in corporate environments with custom CA certificates.
# Alternatively, pass --build-arg GOPROXY=off and ensure vendor/ exists.
COPY vendor/ vendor/
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -ldflags="-s -w" -o /spanner-etcd ./cmd/server/

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /spanner-etcd /spanner-etcd
EXPOSE 2379 2381
ENTRYPOINT ["/spanner-etcd"]
