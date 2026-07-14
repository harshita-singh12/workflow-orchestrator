package grpcapi

import (
	"context"
	"crypto/subtle"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// AuthUnaryInterceptor requires every unary RPC to carry `authorization: Bearer <apiKey>` in
// its request metadata, mirroring the HTTP API's bearer-token check (internal/httpapi) so
// both externally-reachable surfaces get the same protection — the gRPC port is published to
// the host in docker-compose.yml exactly like the HTTP port, so it needs the same guard.
//
// An empty apiKey always rejects every call (fail closed), never "auth disabled".
func AuthUnaryInterceptor(apiKey string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if !validAPIKey(ctx, apiKey) {
			return nil, status.Error(codes.Unauthenticated, "missing or invalid API key")
		}
		return handler(ctx, req)
	}
}

func validAPIKey(ctx context.Context, expected string) bool {
	if expected == "" {
		return false
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	values := md.Get("authorization")
	if len(values) == 0 {
		return false
	}
	const prefix = "Bearer "
	header := values[0]
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	token := strings.TrimPrefix(header, prefix)
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}
