// Package authz implements the RBAC/ABAC authorization layer
// (plans/task/core/23): Go types mirroring deploy/opa/policies/authz.rego's
// input contract, an OPAClient calling a real OPA sidecar's REST API,
// and Connect-RPC/REST middleware enforcing the decision before a
// handler runs. See plans/docs/09-security-compliance.md §10.3.
package authz

// Role is one of the RBAC roles named verbatim in the design
// (plans/docs/09-security-compliance.md §10.3).
type Role string

const (
	RoleTenantAdmin    Role = "tenant_admin"
	RoleReconManager   Role = "recon_manager"
	RoleAnalyst        Role = "analyst"
	RoleApprover       Role = "approver"
	RoleAuditor        Role = "auditor"
	RoleAPIIntegration Role = "api_integration"
)

// Subject is the authenticated caller, threaded through from
// tenancy.TenantContext plus whatever JWT claims/role assignments
// resolved it - kept as a plain struct rather than importing
// internal/tenancy (this package is a platform dependency of
// apiserver/workflow code, not the other way around; see this repo's
// "mirror without importing" convention for structurally-identical
// cross-package shapes).
type Subject struct {
	UserID       string `json:"user_id"`
	TenantID     string `json:"tenant_id"`
	Roles        []Role `json:"roles"`
	BusinessUnit string `json:"business_unit"`
}

// ResourceRef describes the entity an action targets - only the fields
// deploy/opa/policies/authz.rego actually branches on are named here;
// add more as new policies need them.
type ResourceRef struct {
	EntityType          string `json:"entity_type"`
	EntityID            string `json:"entity_id"`
	TenantID            string `json:"tenant_id"`
	AccountBusinessUnit string `json:"account_business_unit"`
	MakerUserID         string `json:"maker_user_id"`
}

// OPAInput is the exact JSON shape sent to OPA's REST API as `input` -
// field names/casing here must match what deploy/opa/policies/authz.rego
// reads (input.subject.roles, input.action, input.resource.maker_user_id,
// etc.) since OPA has no compile-time contract with this struct.
type OPAInput struct {
	Subject  Subject     `json:"subject"`
	Action   string      `json:"action"`
	Resource ResourceRef `json:"resource"`
}

// Decision is OPAClient.Evaluate's result. Reason is populated on deny
// so callers can surface WHY, not a bare 403 - plans/task/core/23's own
// transparency requirement ("the transparency differentiator applies
// to authorization denials too, not just match confidence").
type Decision struct {
	Allow  bool
	Reason string
}
