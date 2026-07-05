// Package tokenization implements the PCI-DSS field tokenization
// technical control (plans/task/core/23, plans/docs/09-security-compliance.md
// §10.2): tag sensitive fields at the ingestion field-mapping stage
// (internal/ingestion/mapping's `tokenize` transform), replace the raw
// value with an opaque token, and keep the token->value mapping in a
// vault store that is NOT the same database/table as the tokenized
// data (colocating them defeats the purpose - see this task's own
// Common Pitfalls). This package implements the technical control
// only: tagging + a real token vault ensuring raw PANs are never
// persisted in raw_payload. It does not claim PCI-DSS scope reduction
// or certification, which is a business/audit process outside code.
package tokenization

import "context"

// TokenizationService tokenizes/detokenizes a tenant-scoped sensitive
// field value. Tokens are per-tenant-namespaced (tenantID is part of
// the vault path, not just an argument threaded through for show) so
// one tenant's vault entries are never reachable via another tenant's
// token even if a token value were somehow guessed/leaked cross-tenant.
type TokenizationService interface {
	Tokenize(ctx context.Context, tenantID, field, value string) (token string, err error)
	Detokenize(ctx context.Context, tenantID, token string) (value string, err error)
}
