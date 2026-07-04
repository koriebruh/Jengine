package tenantcheck_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// Builds the tenantcheck CLI and runs it against the fixture packages
// under testdata/fixtures/ - this IS the "test suite proving it flags a
// violation fixture and passes a compliant one" required by
// plans/task/core/04's Definition of Done. Uses a built binary + exec
// (the same pattern already proven for scripts/lint/check_tenant_id.sh's
// own test) rather than golang.org/x/tools/go/analysis/analysistest,
// since analysistest's GOPATH-style testdata/src convention doesn't mix
// cleanly with fixtures that import real external/in-module packages
// (google/uuid, this module's own tenancy package) the way a plain
// buildable fixture package does.

func buildTenantcheck(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "tenantcheck")

	cmd := exec.Command("go", "build", "-o", bin, "./cmd/tenantcheck")
	cmd.Dir = mustModuleSubdir(t, "internal/platform/lint/tenantcheck")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("failed to build tenantcheck: %v\n%s", err, out)
	}
	return bin
}

// mustModuleSubdir returns the absolute path to <repo-root>/<rel>, locating
// repo root by walking up from the current working directory to the
// directory containing go.mod - same approach as
// internal/testutil.findMigrationFiles.
func mustModuleSubdir(t *testing.T, rel string) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return filepath.Join(dir, rel)
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not locate repo root (go.mod not found)")
		}
		dir = parent
	}
}

func TestTenantcheck_OKFixture(t *testing.T) {
	bin := buildTenantcheck(t)
	cmd := exec.Command(bin, "./testdata/fixtures/ok")
	cmd.Dir = mustModuleSubdir(t, "internal/platform/lint/tenantcheck")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("expected the compliant fixture to pass (exit 0), got error %v\noutput:\n%s", err, out)
	}
}

func TestTenantcheck_ViolationFixture(t *testing.T) {
	bin := buildTenantcheck(t)
	cmd := exec.Command(bin, "./testdata/fixtures/violation")
	cmd.Dir = mustModuleSubdir(t, "internal/platform/lint/tenantcheck")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("expected the violation fixture to be flagged (non-zero exit), got success\noutput:\n%s", out)
	}

	wantSubstrings := []string{
		"BadNoContext: repository method must take context.Context as its first parameter",
		"BadNoTenant: repository method must take an explicit tenantID uuid.UUID parameter or call tenancy.MustTenantFromContext",
	}
	for _, want := range wantSubstrings {
		if !strings.Contains(string(out), want) {
			t.Errorf("expected output to contain %q, got:\n%s", want, out)
		}
	}
}
