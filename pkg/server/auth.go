package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	// tokenTTL matches the etcd default simple token TTL (1 hour).
	// jetcd and other clients do not refresh tokens automatically, so we use
	// a long TTL to avoid unexpected UNAUTHENTICATED errors mid-session.
	tokenTTL      = 1 * time.Hour
	tokenHeader   = "token"
	authHeaderKey = "authorization"
)

// AuthServer implements etcdserverpb.AuthServer.
// Supports simple username/password authentication without RBAC.
// Users are defined via the --auth-users flag: "user1:pass1,user2:pass2".
// When no users are configured, auth is disabled and all requests pass through.
type AuthServer struct {
	etcdserverpb.UnimplementedAuthServer
	mu      sync.RWMutex
	users   map[string]string    // username → password
	tokens  map[string]tokenInfo // token → info
	enabled bool
	log     *zap.Logger
}

type tokenInfo struct {
	username  string
	expiresAt time.Time
}

func newAuthServer(users map[string]string, log *zap.Logger) *AuthServer {
	return &AuthServer{
		users:   users,
		tokens:  make(map[string]tokenInfo),
		enabled: len(users) > 0,
		log:     log,
	}
}

// Authenticate validates credentials and returns a token.
func (a *AuthServer) Authenticate(ctx context.Context, r *etcdserverpb.AuthenticateRequest) (*etcdserverpb.AuthenticateResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.enabled {
		return &etcdserverpb.AuthenticateResponse{Token: "no-auth"}, nil
	}

	pass, ok := a.users[r.Name]
	if !ok || pass != r.Password {
		a.log.Warn("auth failed", zap.String("user", r.Name))
		return nil, status.Error(codes.Unauthenticated, "authentication failed")
	}

	tok := generateToken()
	a.tokens[tok] = tokenInfo{
		username:  r.Name,
		expiresAt: time.Now().Add(tokenTTL),
	}

	a.log.Info("authenticated", zap.String("user", r.Name))
	return &etcdserverpb.AuthenticateResponse{
		Header: &etcdserverpb.ResponseHeader{},
		Token:  tok,
	}, nil
}

// AuthEnable enables authentication (no-op here, controlled by --auth-users).
func (a *AuthServer) AuthEnable(ctx context.Context, r *etcdserverpb.AuthEnableRequest) (*etcdserverpb.AuthEnableResponse, error) {
	return &etcdserverpb.AuthEnableResponse{
		Header: &etcdserverpb.ResponseHeader{},
	}, nil
}

// AuthDisable disables authentication.
func (a *AuthServer) AuthDisable(ctx context.Context, r *etcdserverpb.AuthDisableRequest) (*etcdserverpb.AuthDisableResponse, error) {
	return &etcdserverpb.AuthDisableResponse{
		Header: &etcdserverpb.ResponseHeader{},
	}, nil
}

// AuthStatus returns auth status.
func (a *AuthServer) AuthStatus(ctx context.Context, r *etcdserverpb.AuthStatusRequest) (*etcdserverpb.AuthStatusResponse, error) {
	return &etcdserverpb.AuthStatusResponse{
		Header:  &etcdserverpb.ResponseHeader{},
		Enabled: a.enabled,
	}, nil
}

// validate checks token from gRPC metadata. Returns true if auth is disabled or token is valid.
func (a *AuthServer) validate(ctx context.Context) bool {
	if !a.enabled {
		return true
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}

	// etcd clients send the token as "token" or "authorization" header.
	var tok string
	if vals := md.Get(tokenHeader); len(vals) > 0 {
		tok = vals[0]
	} else if vals := md.Get(authHeaderKey); len(vals) > 0 {
		tok = strings.TrimPrefix(vals[0], "Bearer ")
	}

	if tok == "" {
		return false
	}

	a.mu.RLock()
	info, ok := a.tokens[tok]
	a.mu.RUnlock()

	if !ok || time.Now().After(info.expiresAt) {
		return false
	}
	return true
}

// authUnaryInterceptor validates auth token for unary RPCs.
// Methods in noAuthMethods are always allowed (e.g. Authenticate itself).
func authUnaryInterceptor(auth *AuthServer) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if isPublicMethod(info.FullMethod) || auth.validate(ctx) {
			return handler(ctx, req)
		}
		return nil, status.Error(codes.Unauthenticated, "invalid or expired token")
	}
}

// authStreamInterceptor validates auth token for streaming RPCs.
func authStreamInterceptor(auth *AuthServer) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if isPublicMethod(info.FullMethod) || auth.validate(ss.Context()) {
			return handler(srv, ss)
		}
		return status.Error(codes.Unauthenticated, "invalid or expired token")
	}
}

// isPublicMethod returns true for methods that don't require a token.
func isPublicMethod(method string) bool {
	switch {
	case strings.Contains(method, "Auth/Authenticate"),
		strings.Contains(method, "Auth/AuthEnable"),
		strings.Contains(method, "Auth/AuthDisable"),
		strings.Contains(method, "Auth/AuthStatus"),
		strings.Contains(method, "Health/Check"),
		strings.Contains(method, "grpc.health"):
		return true
	}
	return false
}

// parseUsers parses "user1:pass1,user2:pass2" into a map.
func parseUsers(s string) map[string]string {
	users := make(map[string]string)
	if s == "" {
		return users
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(strings.TrimSpace(pair), ":", 2)
		if len(parts) == 2 && parts[0] != "" {
			users[parts[0]] = parts[1]
		}
	}
	return users
}

func generateToken() string {
	b := make([]byte, 16)
	rand.Read(b) //nolint:errcheck
	return hex.EncodeToString(b)
}
