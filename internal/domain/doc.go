// Package domain holds plain Go structs for every canonical entity
// (plans/docs/03-canonical-data-model.md §4.1) plus the repository
// interfaces (plans/task/core/05) that plans/task/core/06+ build on.
// Repository interfaces live here, not in internal/storage/postgres, so
// callers depend on an abstraction that a future storage backend could
// satisfy differently - see plans/docs/00-overview-and-architecture.md
// §1.3.
package domain
