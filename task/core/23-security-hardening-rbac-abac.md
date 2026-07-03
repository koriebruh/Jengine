# Task 23: Security Hardening — RBAC/ABAC via OPA

## Goal
Build the real authorization layer: the RBAC role hierarchy, an Open Policy Agent (OPA) integration providing ABAC on top of it, encryption at rest/in transit for sensitive data, and the technical control for PCI-DSS field tokenization given that payment-gateway data now flows through the platform (via task 18's webhook-receiver connector). This closes the seam task 20 deliberately left open (the `AuthorizeApprovalActivity` stub) and makes authorization "auditable/adjustable by compliance without a code deploy," per the design's explicit goal.

## Prerequisites
- Core task 15 (API layer — RBAC/ABAC enforcement wraps its handlers).
- Core task 04 (tenancy context — ABAC scoping like "business unit" needs tenant context available).
- Core task 20 (`ApprovalWorkflow`'s `AuthorizeApprovalActivity` stub — this task replaces its internals, not its signature).
- Core task 18 (webhook-receiver connector — the path payment-gateway/settlement data enters through, relevant to the tokenization scope).

## Scope / Deliverables
- `internal/platform/authz/` (already named in the repo layout) — `Role` constants, `Subject`/`Decision`/`OPAInput` types, `OPAClient` interface + implementation, REST/Connect-RPC middleware wiring.
- `deploy/opa/policies/*.rego` — the Rego policy bundle, versioned in-repo.
- `internal/platform/tokenization/` — `TokenizationService` interface + vault-backed implementation.
- Extension to the field-mapping DSL (task 08) adding a `tokenize` transform for tenant-tagged sensitive fields.
- Encryption wiring: `pgcrypto` usage for specifically sensitive columns, S3 SSE-KMS per-tenant KEK usage for object storage writes (building on the DEK/KEK schema fields already present from tenancy work).
- Replacing the stub inside task 20's `AuthorizeApprovalActivity` with a real `OPAClient.Evaluate` call, preserving the Activity's function signature.

## Design Reference
- `plans/docs/09-security-compliance.md` §10.3 (RBAC role list, ABAC/OPA examples), §10.2 (PCI-DSS tokenization scope), §10.1 (encryption — note the audit hash-chain itself is already built in task 14, not redone here).
- `plans/docs/01-multi-tenancy.md` §2.3 (per-tenant DEK/KEK provisioning — the schema/registry fields this task's encryption code actually uses).
- Do not repeat the rationale here.

## Implementation Notes

### RBAC roles (verbatim from the design)
Tenant Admin, Recon Manager, Analyst, Approver, Auditor/Read-Only, API Integration Role — scoped per tenant.
```go
type Role string
const (
    RoleTenantAdmin    Role = "tenant_admin"
    RoleReconManager   Role = "recon_manager"
    RoleAnalyst        Role = "analyst"
    RoleApprover       Role = "approver"
    RoleAuditor        Role = "auditor"
    RoleAPIIntegration Role = "api_integration"
)

type Subject struct {
    UserID       string
    TenantID     string
    Roles        []Role
    BusinessUnit string
}
```

### OPA integration
```go
type Decision struct { Allow bool; Reason string }

type OPAInput struct {
    Subject  Subject
    Action   string      // e.g. "case.assign", "case.approve", "rule.activate"
    Resource ResourceRef  // entity_type, entity_id, tenant_id, account_business_unit, maker_user_id, ...
}

type OPAClient interface {
    Evaluate(ctx context.Context, input OPAInput) (Decision, error)
}
```
Deploy OPA as a sidecar (`opa run --server`) per pod, policy bundle mounted/pulled from an OCI registry or the bundle API, sourced from `deploy/opa/policies/*.rego` in-repo. Add a Connect-RPC interceptor + REST middleware that calls `OPAClient.Evaluate` before the handler executes; deny → 403 with `Decision.Reason` surfaced (not a bare 403 — the transparency differentiator applies to authorization denials too, not just match confidence).

Starter policies (write these as actual `.rego`, not placeholders — they are the two examples the design names explicitly):
```rego
package jengine.authz

default allow = false

# Approver cannot approve own submitted maker action
allow {
  input.action == "case.approve"
  input.subject.roles[_] == "approver"
  input.resource.maker_user_id != input.subject.user_id
}

# Analyst can only act on breaks for accounts in their business unit
allow {
  input.action == "case.act"
  input.subject.roles[_] == "analyst"
  input.resource.account_business_unit == input.subject.business_unit
}
```

### Wiring into task 20's ApprovalWorkflow
Task 20 shipped `AuthorizeApprovalActivity` as a simple role-check stub with a documented seam. Replace its internals to call `OPAClient.Evaluate` instead — the Activity's signature must not change, so `BreakLifecycleWorkflow`/`ApprovalWorkflow` code from task 20 needs zero modification.

### Tokenization (§10.2)
```go
type TokenizationService interface {
    Tokenize(ctx context.Context, tenantID, field, value string) (token string, err error)
    Detokenize(ctx context.Context, tenantID, token string) (value string, err error)
}
```
Applied at the field-mapping stage (task 08) for fields the tenant's mapping spec tags as sensitive, via a new `tokenize` transform function, e.g.:
```yaml
- target: transaction.raw_payload.card_number
  source: field_x
  transform: [tokenize]
```
Token-to-value mapping lives in a separate secured vault store, **not** in the same table/database as the tokenized data — colocating them defeats the purpose. Scope precisely: this task implements the technical control (tagging + vault-based token vault, ensuring raw PANs are never persisted in `raw_payload`) — it does not claim PCI-DSS scope reduction or certification, which is a business/audit process outside code.

### Encryption
- At rest: `pgcrypto` for columns holding especially sensitive data (counterparty PII fields, any tokenization-vault-adjacent columns) — enumerate the specific columns in a migration comment, don't blanket-encrypt everything.
- S3 writes (statement files, audit archive) use SSE-KMS with the per-tenant KEK already provisioned in the tenant registry (per `01-multi-tenancy.md` §2.3) — this task is where that KEK reference actually gets used for envelope encryption at write time, not just stored.
- In transit: verify/configure TLS 1.3 termination at the edge gateway. Full internal service-mesh mTLS (Linkerd) rollout is infra/platform work, not this task's Go code deliverable — flag it as an infra prerequisite tracked outside this task list, don't attempt to install a service mesh here.

## Non-Goals / Guardrails
- Do not implement Linkerd/service-mesh installation — infra-team work, out of scope for this task's code deliverables.
- Do not implement SOC2 Type II certification/audit itself — V2 per the roadmap, see task 26.
- Do not implement data-residency/region-pinning or Citus isolation tiers — that is task 24's territory (this task answers "who can do what," task 24 answers "where does the data live").
- Do not hardcode role checks as Go `if`/`else` chains bypassing OPA — defeats the explicit design goal of compliance-editable policy without code deploy.
- Do not add a second maker/checker check anywhere else (e.g. in the UI layer) as a substitute for the workflow-level enforcement already built in task 20 — this task strengthens that single enforcement point, it doesn't duplicate it.

## Definition of Done
- `opa test` unit tests for the Rego policy bundle covering both example policies plus edge cases (approver == maker rejected; analyst acting outside their business unit rejected; auditor role cannot perform any mutating action).
- Integration test verifying the middleware actually blocks/allows real API calls per policy (not just the Rego test suite in isolation).
- Tokenization round-trip test: tokenize→detokenize returns the original value, tokens are structurally distinct from valid PAN-shaped strings, and the raw value never appears in logs.
- Encryption test: querying the raw bytes of a `pgcrypto`-protected column returns ciphertext, not a plaintext match.
- Manual verification: an Approver attempting to approve their own submitted write-off is rejected with a clear reason string.

## Common Pitfalls
- Bypassing OPA with inline Go authorization logic "just for this one endpoint."
- Changing `AuthorizeApprovalActivity`'s signature while swapping its internals, forcing an unnecessary change to task 20's workflow code.
- Storing the tokenization vault mapping in the same database/table as the tokenized data.
- Logging raw sensitive values anywhere in the tokenization code path for debugging purposes.
- Attempting to stand up a full service mesh as part of this task instead of flagging it as an infra dependency.
- Conflating this task's scope with task 24's — this is authorization and encryption, not tenant isolation tiers or data residency routing.
