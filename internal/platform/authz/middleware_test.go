package authz

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/tenancy"
)

type fakeOPAClient struct {
	decision Decision
	err      error
	lastCall OPAInput
}

func (f *fakeOPAClient) Evaluate(ctx context.Context, input OPAInput) (Decision, error) {
	f.lastCall = input
	return f.decision, f.err
}

func contextWithTenant(userID string, roles []string, businessUnit string) context.Context {
	tenantID := uuid.New()
	return tenancy.WithTenant(context.Background(), tenancy.TenantContext{
		TenantID: tenantID, UserID: userID, Roles: roles, BusinessUnit: businessUnit,
	})
}

func TestSubjectFromContext(t *testing.T) {
	ctx := contextWithTenant("user-1", []string{"analyst"}, "bu1")
	subject := SubjectFromContext(ctx)
	if subject.UserID != "user-1" || subject.BusinessUnit != "bu1" || len(subject.Roles) != 1 || subject.Roles[0] != RoleAnalyst {
		t.Errorf("unexpected subject: %+v", subject)
	}
}

func TestMiddleware_Authorize_Allow(t *testing.T) {
	client := &fakeOPAClient{decision: Decision{Allow: true}}
	m := NewMiddleware(client)

	ctx := contextWithTenant("user-1", []string{"tenant_admin"}, "bu1")
	if err := m.Authorize(ctx, "rule.activate", ResourceRef{MakerUserID: "someone-else"}); err != nil {
		t.Errorf("expected nil error on allow, got %v", err)
	}
	if client.lastCall.Action != "rule.activate" {
		t.Errorf("expected action passed through to OPA, got %q", client.lastCall.Action)
	}
}

func TestMiddleware_Authorize_Deny(t *testing.T) {
	client := &fakeOPAClient{decision: Decision{Allow: false, Reason: "maker != checker violation"}}
	m := NewMiddleware(client)

	ctx := contextWithTenant("user-1", []string{"tenant_admin"}, "bu1")
	err := m.Authorize(ctx, "rule.activate", ResourceRef{MakerUserID: "user-1"})
	if err == nil {
		t.Fatal("expected an error on deny")
	}
}
