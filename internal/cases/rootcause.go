package cases

// DefaultRootCauseCategories seeds the default taxonomy from
// plans/docs/05-case-management.md §6.6 - a simple, tenant-extensible
// lookup (not a workflow-critical piece at MVP, per that section: no
// rule-suggestion engine consumes these yet).
var DefaultRootCauseCategories = []string{
	"Timing Difference",
	"Data Entry Error",
	"Duplicate Transaction",
	"FX Rate Variance",
	"Missing Counterparty Statement",
	"System Interface Failure",
	"Fraud/Investigation",
	"Fee/Charge Discrepancy",
	"Unauthorized Transaction",
}

// IsValidRootCause reports whether category is one of the default
// taxonomy's categories. Tenant-added custom categories (§6.6: "a
// tenant can add custom categories") aren't validated here - this
// package has no tenant-settings dependency; a caller wiring a custom
// per-tenant taxonomy should check against its own list before calling
// TagRootCause, or extend this check with a tenant-scoped lookup once
// that storage exists.
func IsValidRootCause(category string) bool {
	for _, c := range DefaultRootCauseCategories {
		if c == category {
			return true
		}
	}
	return false
}
