// Package violation is a deliberately non-compliant fixture for
// tenantcheck_test.go - tenantcheck must flag both methods below.
package violation

import "context"

type Repo struct{}

func (r *Repo) BadNoContext(x int) error {
	return nil
}

func (r *Repo) BadNoTenant(ctx context.Context, id string) error {
	return nil
}
