FROM golang:1.26-alpine AS builder
WORKDIR /src
# Copy dependency manifests first for better layer caching.
COPY go.mod go.sum ./
# If building inside a corporate network with a custom CA or GOPROXY, pass:
#   --build-arg GOPROXY=https://... --build-arg GONOSUMCHECK=*
ARG GOPROXY=https://proxy.golang.org,direct
ARG GONOSUMCHECK=""
ARG GONOSUMDB=""
RUN GOPROXY=${GOPROXY} GONOSUMCHECK=${GONOSUMCHECK} GONOSUMDB=${GONOSUMDB} \
    go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -ldflags="-s -w" -o /spanner-etcd ./cmd/server/

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /spanner-etcd /spanner-etcd
EXPOSE 2379 2381
ENTRYPOINT ["/spanner-etcd"]
