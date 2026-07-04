package apiserver

import (
	"net/http"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

// WrapAuth resolves tenant identity (JWT bearer token or API key) for
// every request before any Connect-RPC handler runs, injecting
// TenantContext into the request's context.Context - exactly
// tenancy.Middleware.Wrap (plans/task/core/04), reused as-is rather than
// reimplemented (plans/task/core/15 Implementation Notes: "delegates to
// task 04's tenancy package... this task wires it into the HTTP/Connect
// layer, doesn't reimplement it"). Connect-RPC handlers are served as
// plain http.Handlers under the hood, so this net/http middleware
// composes with them with no adapter needed - the context set here
// flows into every handler via (*http.Request).Context().
//
// Actor identity (cases.Actor{UserID, Role} - who is performing a
// lifecycle action, for audit attribution) is a client-supplied request
// field at MVP, not derived from the authenticated JWT here - MVP auth
// is intentionally coarse (plans/task/core/15 Non-Goals: no OIDC/SAML
// SSO, no OPA/ABAC), and tenancy.Claims itself has no user-identity claim
// yet (its own doc comment: "room is left for an actor/role claim
// later"). Cryptographically binding Actor to the authenticated caller
// is task 23/V1's RBAC/ABAC enforcement, not built here.
func WrapAuth(mw *tenancy.Middleware, next http.Handler) http.Handler {
	return mw.Wrap(next)
}
