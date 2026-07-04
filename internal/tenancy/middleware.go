package tenancy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrMissingAuth   = errors.New("tenancy: missing authentication")
	ErrMalformedAuth = errors.New("tenancy: malformed Authorization header")
	ErrInvalidToken  = errors.New("tenancy: invalid or expired token")
)

// Claims is the JWT claim set this middleware reads. Room is left for an
// actor/role claim later (plans/docs/09-security-compliance.md §10.3)
// without a breaking change to this struct - RBAC/OPA evaluation itself is
// plans/task/core/23 (V1), not built here; see Non-Goals.
type Claims struct {
	jwt.RegisteredClaims
	TenantID string `json:"tenant_id"`
}

// Middleware resolves tenant identity per request (JWT bearer token or
// API key), loads routing info from the registry, and injects
// TenantContext into the request context. It does not touch the DB/RLS
// session variable itself - see WithTenantTx for that half of the
// contract (the two are deliberately separate: this runs once per HTTP
// request, WithTenantTx runs once per DB transaction, and a single
// request may open more than one transaction).
type Middleware struct {
	registry  RegistryRepo
	jwtSecret []byte
}

func NewMiddleware(registry RegistryRepo, jwtSecret []byte) *Middleware {
	return &Middleware{registry: registry, jwtSecret: jwtSecret}
}

// Wrap returns an http.Handler that resolves the tenant before calling
// next, or responds 401 without calling next if resolution fails -
// invalid/missing credentials never reach application code or the DB.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tenant, err := m.resolveTenant(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusUnauthorized)
			return
		}

		tc := TenantContext{
			TenantID:      tenant.ID,
			IsolationTier: tenant.IsolationTier,
			Region:        tenant.Region,
		}

		// Isolation config is optional at MVP (every tenant resolves to
		// the single local Standard-tier Postgres instance regardless -
		// plans/task/core/24 is where ShardID/SchemaName start mattering).
		// Its absence is not a request failure.
		if cfg, cfgErr := m.registry.GetIsolationConfig(r.Context(), tenant.ID); cfgErr == nil {
			tc.ShardID = cfg.ShardID
			tc.SchemaName = cfg.SchemaName
		}

		ctx := WithTenant(r.Context(), tc)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Middleware) resolveTenant(r *http.Request) (Tenant, error) {
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return m.resolveByAPIKey(r.Context(), apiKey)
	}

	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		return Tenant{}, ErrMissingAuth
	}
	tokenStr, found := strings.CutPrefix(authHeader, "Bearer ")
	if !found {
		return Tenant{}, ErrMalformedAuth
	}
	return m.resolveByJWT(r.Context(), tokenStr)
}

func (m *Middleware) resolveByJWT(ctx context.Context, tokenStr string) (Tenant, error) {
	token, err := jwt.ParseWithClaims(tokenStr, &Claims{}, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("tenancy: unexpected signing method %v", t.Header["alg"])
		}
		return m.jwtSecret, nil
	})
	if err != nil || !token.Valid {
		return Tenant{}, ErrInvalidToken
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || claims.TenantID == "" {
		return Tenant{}, ErrInvalidToken
	}
	tenantID, err := uuid.Parse(claims.TenantID)
	if err != nil {
		return Tenant{}, ErrInvalidToken
	}
	tenant, err := m.registry.GetTenant(ctx, tenantID)
	if err != nil {
		return Tenant{}, ErrInvalidToken
	}
	return tenant, nil
}

func (m *Middleware) resolveByAPIKey(ctx context.Context, apiKey string) (Tenant, error) {
	sum := sha256.Sum256([]byte(apiKey))
	hash := hex.EncodeToString(sum[:])
	tenant, err := m.registry.GetTenantByAPIKeyHash(ctx, hash)
	if err != nil {
		return Tenant{}, ErrInvalidToken
	}
	return tenant, nil
}

// WithTenantTx runs fn inside a Postgres transaction that has had
// app.current_tenant_id set for the transaction's lifetime only, via
// set_config(..., true) - the third argument, is_local=true, makes this
// behave exactly like SET LOCAL. This is deliberately NOT a bare
// SET/set_config(..., false): on a pooled connection, a non-local setting
// persists after the transaction ends and leaks into whichever tenant's
// request reuses that connection next - see plans/task/core/04 Common
// Pitfalls, this is the single most dangerous mistake this file could
// make. set_config is used instead of a string-interpolated
// "SET LOCAL app.current_tenant_id = '...'" so the tenant ID is a bound
// query parameter, not a string concatenated into SQL.
//
// Uses MustTenantFromContext deliberately (see context.go): calling this
// without a tenant already in ctx is a programming error, not a
// recoverable condition.
func WithTenantTx(ctx context.Context, pool *pgxpool.Pool, fn func(ctx context.Context, tx pgx.Tx) error) error {
	tc := MustTenantFromContext(ctx)

	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("tenancy: begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op if already committed

	if _, err := tx.Exec(ctx, `SELECT set_config('app.current_tenant_id', $1, true)`, tc.TenantID.String()); err != nil {
		return fmt.Errorf("tenancy: set_config app.current_tenant_id: %w", err)
	}

	if err := fn(ctx, tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
