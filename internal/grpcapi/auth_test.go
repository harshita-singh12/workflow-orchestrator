package grpcapi

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func withAuthHeader(value string) context.Context {
	ctx := context.Background()
	if value == "" {
		return ctx
	}
	return metadata.NewIncomingContext(ctx, metadata.Pairs("authorization", value))
}

func callHandler(t *testing.T, apiKey string, ctx context.Context) error {
	t.Helper()
	interceptor := AuthUnaryInterceptor(apiKey)
	called := false
	_, err := interceptor(ctx, nil, nil, func(ctx context.Context, req any) (any, error) {
		called = true
		return "ok", nil
	})
	if err == nil {
		require.True(t, called, "handler should run when auth succeeds")
	} else {
		require.False(t, called, "handler must not run when auth fails")
	}
	return err
}

func TestAuthInterceptor_MissingMetadataRejected(t *testing.T) {
	err := callHandler(t, "test-key", context.Background())
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthInterceptor_InvalidKeyRejected(t *testing.T) {
	err := callHandler(t, "test-key", withAuthHeader("Bearer wrong-key"))
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

func TestAuthInterceptor_MalformedHeaderRejected(t *testing.T) {
	cases := []string{"test-key", "Bearer", "Basic test-key"}
	for _, h := range cases {
		err := callHandler(t, "test-key", withAuthHeader(h))
		require.Errorf(t, err, "header %q should be rejected", h)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	}
}

func TestAuthInterceptor_ValidKeyAccepted(t *testing.T) {
	err := callHandler(t, "test-key", withAuthHeader("Bearer test-key"))
	require.NoError(t, err)
}

func TestAuthInterceptor_EmptyConfiguredKeyFailsClosed(t *testing.T) {
	err := callHandler(t, "", withAuthHeader("Bearer "))
	require.Error(t, err)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}
