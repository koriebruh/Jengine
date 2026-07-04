package ok

import "context"

type Relay struct{}

// tenantcheck:exempt - test fixture proving the marker mechanism
// itself: this method has neither a tenantID param nor a
// MustTenantFromContext call, and must still pass because of the marker.
func (r *Relay) SweepAllTenants(ctx context.Context, limit int) error {
	return nil
}
