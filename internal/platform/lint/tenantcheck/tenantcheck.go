// Package tenantcheck implements the tenancy lint enforcement mechanism
// for plans/docs/01-multi-tenancy.md §2.2: "repository queries require
// explicit non-nil tenant_id, enforced by lint." Supersedes
// plans/task/core/01's grep-based scripts/lint/check_tenant_id.sh now
// that a real convention exists (plans/task/core/04) - a go/analysis
// checker catches what a grep pattern can't (e.g. distinguishing an
// actual parameter from a comment mentioning "tenantID").
package tenantcheck

import (
	"go/ast"
	"strings"

	"golang.org/x/tools/go/analysis"
)

const doc = `check that repository-layer methods take an explicit tenant scope

Flags exported methods whose signature doesn't take context.Context as
the first parameter, and whose body neither takes an explicit tenantID
uuid.UUID parameter nor calls tenancy.MustTenantFromContext.

Scope is caller-controlled: point this analyzer at whichever package(s)
constitute the repository layer (plans/task/core/05), e.g.
"./internal/storage/postgres/...". internal/tenancy/registry.go is always
skipped entirely - see plans/task/core/04's RegistryRepo doc comment for
why (it is the Tenant Registry lookup repo, which cannot itself take a
"current tenant" parameter since its job is looking tenants up by ID/API
key).

A single method elsewhere can opt out of this check by including
"tenantcheck:exempt" in its doc comment, followed by a reason - used
sparingly, for two known shapes of legitimate exception:
  - Genuinely cross-tenant infrastructure methods (e.g.
    internal/storage/postgres's OutboxRepo.ListUnsent/MarkSent, which
    sweep all tenants' outbox rows and must run against an
    RLS-bypassing connection, not a tenant-scoped one).
  - Methods whose signature is fixed by an unrelated interface this
    type must implement (e.g. internal/storage/postgres's
    PersistEmitStage.Name/Process, which satisfy
    internal/ingestion/pipeline.Stage - tenant scoping there happens
    via a struct field used inside a WithTx call, not a method
    parameter, because the Stage interface itself has no room for one).
See plans/task/core/06.`

var Analyzer = &analysis.Analyzer{
	Name: "tenantcheck",
	Doc:  doc,
	Run:  run,
}

const exemptMarker = "tenantcheck:exempt"

func run(pass *analysis.Pass) (interface{}, error) {
	for _, file := range pass.Files {
		filename := pass.Fset.Position(file.Pos()).Filename
		if strings.HasSuffix(filename, "internal/tenancy/registry.go") {
			continue
		}
		if strings.HasSuffix(filename, "_test.go") {
			continue
		}

		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Recv == nil { // methods only, not free functions
				continue
			}
			if !fn.Name.IsExported() {
				continue
			}
			if fn.Doc != nil && strings.Contains(fn.Doc.Text(), exemptMarker) {
				continue
			}
			checkFunc(pass, fn)
		}
	}
	return nil, nil
}

func checkFunc(pass *analysis.Pass, fn *ast.FuncDecl) {
	params := fn.Type.Params.List
	if len(params) == 0 || !isContextType(pass, params[0].Type) {
		pass.Reportf(fn.Pos(), "%s: repository method must take context.Context as its first parameter", fn.Name.Name)
		return
	}

	if hasTenantIDParam(pass, params) {
		return
	}
	if callsMustTenantFromContext(fn.Body) {
		return
	}

	pass.Reportf(fn.Pos(), "%s: repository method must take an explicit tenantID uuid.UUID parameter or call tenancy.MustTenantFromContext", fn.Name.Name)
}

func isContextType(pass *analysis.Pass, expr ast.Expr) bool {
	t := pass.TypesInfo.TypeOf(expr)
	return t != nil && t.String() == "context.Context"
}

func hasTenantIDParam(pass *analysis.Pass, params []*ast.Field) bool {
	for _, p := range params {
		t := pass.TypesInfo.TypeOf(p.Type)
		if t == nil || t.String() != "github.com/google/uuid.UUID" {
			continue
		}
		for _, name := range p.Names {
			if name.Name == "tenantID" {
				return true
			}
		}
	}
	return false
}

func callsMustTenantFromContext(body *ast.BlockStmt) bool {
	if body == nil {
		return false
	}
	found := false
	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		sel, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if sel.Sel.Name == "MustTenantFromContext" {
			found = true
			return false
		}
		return true
	})
	return found
}
