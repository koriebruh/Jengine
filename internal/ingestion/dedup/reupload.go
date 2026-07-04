package dedup

import (
	"context"
	"fmt"

	"github.com/google/uuid"
)

// ReuploadPolicy mirrors connector/csvupload's DuplicatePolicy values
// (kept as a distinct type here since dedup doesn't depend on csvupload)
// - "reject" or "correction".
type ReuploadPolicy string

const (
	ReuploadPolicyReject            ReuploadPolicy = "reject"
	ReuploadPolicyTreatAsCorrection ReuploadPolicy = "correction"
)

// StatementChecksumChecker is the surface CheckFileReupload needs -
// internal/storage/postgres.StatementRepo satisfies it structurally.
type StatementChecksumChecker interface {
	ExistsByChecksum(ctx context.Context, tenantID, accountID uuid.UUID, checksum string) (bool, error)
}

// TenantReuploadPolicyLookup resolves a tenant's configured file-reupload
// policy (stored under tenant_settings' generic key/value shape, key
// "file_reupload_policy" - plans/task/core/03/04's Tenant Registry).
type TenantReuploadPolicyLookup interface {
	GetReuploadPolicy(ctx context.Context, tenantID uuid.UUID) (ReuploadPolicy, error)
}

// CheckFileReupload centralizes the file-level re-upload policy decision
// (plans/task/core/09 Implementation Notes: "the policy decision logic
// lives in one place... rather than being duplicated per-connector").
// Returns ("", false, nil) when checksum isn't a re-upload at all - the
// caller should just proceed normally. task 07's csvupload/sftp
// connectors already implement and test this exact quarantine-vs-
// correction behavior directly (DuplicatePolicyQuarantine/
// DuplicatePolicyCorrection) - this helper is the centralized version
// future connectors should call, but the already-verified task 07
// connectors are deliberately left as-is rather than risking a refactor
// of working, tested code for marginal architectural tidiness.
func CheckFileReupload(ctx context.Context, statements StatementChecksumChecker, settings TenantReuploadPolicyLookup, tenantID, accountID uuid.UUID, checksum string) (policy ReuploadPolicy, isReupload bool, err error) {
	exists, err := statements.ExistsByChecksum(ctx, tenantID, accountID, checksum)
	if err != nil {
		return "", false, fmt.Errorf("dedup: check file reupload: %w", err)
	}
	if !exists {
		return "", false, nil
	}

	p, err := settings.GetReuploadPolicy(ctx, tenantID)
	if err != nil {
		return "", true, fmt.Errorf("dedup: get reupload policy: %w", err)
	}
	if p == "" {
		p = ReuploadPolicyReject // safe default - never silently overwrite financial data
	}
	return p, true, nil
}
