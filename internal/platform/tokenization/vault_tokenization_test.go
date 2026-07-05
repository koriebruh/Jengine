package tokenization_test

import (
	"context"
	"net"
	"regexp"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/platform/tokenization"
)

const (
	localVaultAddr  = "localhost:8200"
	localVaultURL   = "http://localhost:8200"
	localVaultToken = "jengine-dev-root-token"
)

func requireLocalVault(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", localVaultAddr, 2*time.Second)
	if err != nil {
		t.Skipf("local Vault not reachable at %s (run `docker compose up -d vault`): %v", localVaultAddr, err)
	}
	_ = conn.Close()
}

// panShapedPattern matches a plausible raw PAN (13-19 digits) - used to
// assert a minted token is structurally distinct from one, per this
// task's own DoD wording.
var panShapedPattern = regexp.MustCompile(`^[0-9]{13,19}$`)

func TestVaultTokenizationService_RoundTrip(t *testing.T) {
	requireLocalVault(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	svc := tokenization.NewVaultTokenizationService(localVaultURL, localVaultToken)
	tenantID := uuid.New().String()
	const cardNumber = "4111111111111111"

	token, err := svc.Tokenize(ctx, tenantID, "card_number", cardNumber)
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	if panShapedPattern.MatchString(token) {
		t.Errorf("token %q is structurally indistinguishable from a raw PAN - it must not be", token)
	}
	if token == cardNumber {
		t.Error("token must not equal the raw value")
	}

	got, err := svc.Detokenize(ctx, tenantID, token)
	if err != nil {
		t.Fatalf("Detokenize failed: %v", err)
	}
	if got != cardNumber {
		t.Errorf("Detokenize returned %q, want original value %q", got, cardNumber)
	}
}

func TestVaultTokenizationService_TenantIsolation(t *testing.T) {
	requireLocalVault(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	svc := tokenization.NewVaultTokenizationService(localVaultURL, localVaultToken)
	tenantA := uuid.New().String()
	tenantB := uuid.New().String()

	token, err := svc.Tokenize(ctx, tenantA, "card_number", "4000000000000002")
	if err != nil {
		t.Fatalf("Tokenize failed: %v", err)
	}

	if _, err := svc.Detokenize(ctx, tenantB, token); err == nil {
		t.Error("expected detokenizing tenant A's token under tenant B's namespace to fail, it did not")
	}
}

func TestVaultTokenizationService_UnknownTokenFails(t *testing.T) {
	requireLocalVault(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	svc := tokenization.NewVaultTokenizationService(localVaultURL, localVaultToken)
	if _, err := svc.Detokenize(ctx, uuid.New().String(), "tok_nonexistent"); err == nil {
		t.Error("expected detokenizing an unknown token to fail, it did not")
	}
}
