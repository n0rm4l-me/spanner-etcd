package server

import (
	"context"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
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

// loggingUnary logs slow unary RPCs.
func loggingUnary(log *zap.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(start)
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

// loggingStream logs stream RPC lifecycle.
func loggingStream(log *zap.Logger) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		start := time.Now()
		err := handler(srv, ss)
		log.Debug("stream rpc",
			zap.String("method", info.FullMethod),
			zap.Duration("elapsed", time.Since(start)),
			zap.Error(err),
		)
		return err
	}
}
