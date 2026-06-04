package server

import (
	"context"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/n0rm4l-me/spanner-etcd/pkg/metrics"
)

// contextWithCancel is a helper to create a cancellable context from an interface.
func contextWithCancel(parent interface{ Done() <-chan struct{} }) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// streamContextWithCancel creates a Go context from a gRPC stream context interface.
func streamContextWithCancel(parent interface {
	Done() <-chan struct{}
	Err() error
}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-parent.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

// loggingUnary logs slow unary RPCs and records Prometheus metrics.
func loggingUnary(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)

		status := "ok"
		if err != nil {
			status = "error"
		}
		method := shortMethod(info.FullMethod)
		metrics.RPCDuration.WithLabelValues(method, status).Observe(elapsed.Seconds())

		if elapsed > 500*time.Millisecond || err != nil {
			log.Info("unary rpc",
				zap.String("method", info.FullMethod),
				zap.Duration("elapsed", elapsed),
				zap.Error(err),
			)
		}
		return resp, err
	}
}

// loggingStream logs stream RPC lifecycle and records Prometheus metrics.
func loggingStream(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		elapsed := time.Since(start)

		status := "ok"
		if err != nil {
			status = "error"
		}
		metrics.RPCDuration.WithLabelValues(shortMethod(info.FullMethod), status).Observe(elapsed.Seconds())

		log.Debug("stream rpc",
			zap.String("method", info.FullMethod),
			zap.Duration("elapsed", elapsed),
			zap.Error(err),
		)
		return err
	}
}

// shortMethod strips the package prefix from a full gRPC method name.
// "/etcdserverpb.KV/Range" → "KV/Range"
func shortMethod(full string) string {
	if idx := strings.LastIndex(full, "."); idx >= 0 {
		return full[idx+1:]
	}
	return strings.TrimPrefix(full, "/")
}
