package tenancy_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

// fakeRegistry is an in-memory RegistryRepo for unit-testing Middleware
// without needing a real Postgres - the DB-backed behavior of each method
// is already covered by registry_test.go's integration tests.
type fakeRegistry struct {
	tenantsByID     map[uuid.UUID]tenancy.Tenant
	tenantsByAPIKey map[string]tenancy.Tenant
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{
		tenantsByID:     map[uuid.UUID]tenancy.Tenant{},
		tenantsByAPIKey: map[string]tenancy.Tenant{},
	}
}

func (f *fakeRegistry) GetTenant(_ context.Context, tenantID uuid.UUID) (tenancy.Tenant, error) {
	t, ok := f.tenantsByID[tenantID]
	if !ok {
		return tenancy.Tenant{}, tenancy.ErrNotFound
	}
	return t, nil
}

func (f *fakeRegistry) GetTenantByAPIKeyHash(_ context.Context, hash string) (tenancy.Tenant, error) {
	t, ok := f.tenantsByAPIKey[hash]
	if !ok {
		return tenancy.Tenant{}, tenancy.ErrNotFound
	}
	return t, nil
}

func (f *fakeRegistry) GetIsolationConfig(_ context.Context, tenantID uuid.UUID) (tenancy.IsolationConfig, error) {
	return tenancy.IsolationConfig{}, tenancy.ErrNotFound
}

func (f *fakeRegistry) GetQuota(_ context.Context, tenantID uuid.UUID) (tenancy.Quota, error) {
	return tenancy.Quota{}, tenancy.ErrNotFound
}

func (f *fakeRegistry) IsFeatureEnabled(_ context.Context, tenantID uuid.UUID, flag string) (bool, error) {
	return false, nil
}

func signTestJWT(t *testing.T, secret []byte, tenantID string, expiresAt time.Time) string {
	t.Helper()
	claims := tenancy.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
		},
		TenantID: tenantID,
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(secret)
	if err != nil {
		t.Fatalf("failed to sign test JWT: %v", err)
	}
	return signed
}

func TestMiddleware_JWTAuth(t *testing.T) {
	secret := []byte("test-secret")
	tenantID := uuid.New()
	registry := newFakeRegistry()
	registry.tenantsByID[tenantID] = tenancy.Tenant{ID: tenantID, Name: "Acme", IsolationTier: tenancy.IsolationTierStandard}

	mw := tenancy.NewMiddleware(registry, secret)

	var gotTenant tenancy.TenantContext
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant, _ = tenancy.TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := mw.Wrap(inner)

	t.Run("valid token resolves tenant and calls next", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signTestJWT(t, secret, tenantID.String(), time.Now().Add(time.Hour)))

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if gotTenant.TenantID != tenantID {
			t.Errorf("expected TenantContext.TenantID %s, got %s", tenantID, gotTenant.TenantID)
		}
	})

	t.Run("expired token rejected before reaching next", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signTestJWT(t, secret, tenantID.String(), time.Now().Add(-time.Hour)))

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for expired token, got %d", rec.Code)
		}
	})

	t.Run("wrong signing secret rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signTestJWT(t, []byte("wrong-secret"), tenantID.String(), time.Now().Add(time.Hour)))

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for wrong signature, got %d", rec.Code)
		}
	})

	t.Run("missing Authorization header rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for missing auth, got %d", rec.Code)
		}
	})

	t.Run("token for unknown tenant rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("Authorization", "Bearer "+signTestJWT(t, secret, uuid.New().String(), time.Now().Add(time.Hour)))

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for unknown tenant, got %d", rec.Code)
		}
	})
}

func TestMiddleware_APIKeyAuth(t *testing.T) {
	tenantID := uuid.New()
	registry := newFakeRegistry()
	apiKey := "test-api-key"
	sum := sha256.Sum256([]byte(apiKey))
	hash := hex.EncodeToString(sum[:])
	registry.tenantsByAPIKey[hash] = tenancy.Tenant{ID: tenantID, Name: "Acme"}

	mw := tenancy.NewMiddleware(registry, []byte("unused"))

	var gotTenant tenancy.TenantContext
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotTenant, _ = tenancy.TenantFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	handler := mw.Wrap(inner)

	t.Run("valid API key resolves tenant", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", apiKey)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", rec.Code)
		}
		if gotTenant.TenantID != tenantID {
			t.Errorf("expected TenantContext.TenantID %s, got %s", tenantID, gotTenant.TenantID)
		}
	})

	t.Run("unknown API key rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		req.Header.Set("X-API-Key", "not-a-real-key")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("expected 401 for unknown API key, got %d", rec.Code)
		}
	})
}
