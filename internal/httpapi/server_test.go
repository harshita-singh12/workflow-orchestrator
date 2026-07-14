package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/aryanraj/workflow-orchestrator/internal/engine"
	memq "github.com/aryanraj/workflow-orchestrator/internal/queue/memory"
	mems "github.com/aryanraj/workflow-orchestrator/internal/store/memory"
)

const testAPIKey = "test-api-key-12345"

// newTestServer builds a Server against an in-process store/queue, exactly like
// internal/engine's own tests — no Postgres/Redis needed. Registry/Scheduler are left nil,
// which every handler already tolerates (see handleListWorkers/handleClusterStatus).
func newTestServer(t *testing.T, apiKey string, allowedOrigins []string) *Server {
	t.Helper()
	s := mems.New()
	q := memq.New()
	e := engine.New(s, q, nil)
	return New(e, s, nil, nil, nil, apiKey, allowedOrigins)
}

func TestAuth_MissingHeaderRejected(t *testing.T) {
	srv := newTestServer(t, testAPIKey, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/definitions", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "application/json")
	require.Contains(t, rec.Body.String(), "error")
}

func TestAuth_InvalidKeyRejected(t *testing.T) {
	srv := newTestServer(t, testAPIKey, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/definitions", nil)
	req.Header.Set("Authorization", "Bearer wrong-key")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestAuth_MalformedHeaderRejected(t *testing.T) {
	srv := newTestServer(t, testAPIKey, nil)

	cases := []string{
		testAPIKey,             // no "Bearer " prefix at all
		"Bearer",               // prefix with no token, no trailing space
		"Basic " + testAPIKey,  // wrong scheme
		"bearer " + testAPIKey, // wrong case
		"",                     // empty header value
	}
	for _, header := range cases {
		req := httptest.NewRequest(http.MethodGet, "/api/definitions", nil)
		if header != "" {
			req.Header.Set("Authorization", header)
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		require.Equalf(t, http.StatusUnauthorized, rec.Code, "header %q should be rejected", header)
	}
}

func TestAuth_ValidKeyAccepted(t *testing.T) {
	srv := newTestServer(t, testAPIKey, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/definitions", nil)
	req.Header.Set("Authorization", "Bearer "+testAPIKey)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
}

// TestAuth_EmptyConfiguredKeyFailsClosed guards against the classic "empty expected secret
// means auth is off" bug: an empty configured key must reject every request, including one
// that (mis)matches it with an empty bearer token.
func TestAuth_EmptyConfiguredKeyFailsClosed(t *testing.T) {
	srv := newTestServer(t, "", nil)

	req := httptest.NewRequest(http.MethodGet, "/api/definitions", nil)
	req.Header.Set("Authorization", "Bearer ")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestHealthz_NoAuthRequired(t *testing.T) {
	srv := newTestServer(t, testAPIKey, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Body.String(), "ok")
}

func TestCORS_ReflectsOnlyConfiguredOrigin(t *testing.T) {
	srv := newTestServer(t, testAPIKey, []string{"http://localhost:3002"})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://localhost:3002")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Equal(t, "http://localhost:3002", rec.Header().Get("Access-Control-Allow-Origin"))
	require.NotEqual(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_UnknownOriginNotReflected(t *testing.T) {
	srv := newTestServer(t, testAPIKey, []string{"http://localhost:3002"})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://evil.example")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
}

func TestCORS_NeverWildcard(t *testing.T) {
	// Even with no configured allowlist at all, the middleware must never fall back to "*".
	srv := newTestServer(t, testAPIKey, nil)

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("Origin", "http://anything.example")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	require.NotEqual(t, "*", rec.Header().Get("Access-Control-Allow-Origin"))
	require.Empty(t, rec.Header().Get("Access-Control-Allow-Origin"))
}
