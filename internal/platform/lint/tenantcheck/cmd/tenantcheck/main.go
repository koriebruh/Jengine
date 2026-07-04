// Command tenantcheck runs the tenancy lint analyzer as a standalone CLI,
// e.g.:
//
//	go run ./internal/platform/lint/tenantcheck/cmd/tenantcheck ./internal/storage/postgres/...
//
// Wired into `make lint-tenancy` (plans/task/core/04).
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/koriebruh/Jengine/internal/platform/lint/tenantcheck"
)

func main() {
	singlechecker.Main(tenantcheck.Analyzer)
}
