FROM golang:1.26-alpine AS builder

WORKDIR /src

# Copy dependency manifests first for better layer caching.
COPY go.mod go.sum ./

# Copy vendor if it exists (created by `make vendor` or `go mod vendor`).
# In corporate environments with custom CA, run `make vendor` locally first.
# Without vendor, set GOPROXY/GONOSUMDB to an accessible proxy.
COPY vendor/ vendor/

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -mod=vendor -ldflags="-s -w" -o /spanner-etcd ./cmd/server/ && \
    go build -mod=vendor -ldflags="-s -w" -o /spanner-etcd-migrate ./cmd/migrate/

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /spanner-etcd /spanner-etcd
COPY --from=builder /spanner-etcd-migrate /spanner-etcd-migrate
EXPOSE 2379 2381
ENTRYPOINT ["/spanner-etcd"]
