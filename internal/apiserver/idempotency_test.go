package apiserver_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/internal/apiserver"
)

type spyStore struct {
	entries map[string]apiserver.StoredResponse
}

func newSpyStore() *spyStore {
	return &spyStore{entries: make(map[string]apiserver.StoredResponse)}
}

func (s *spyStore) Get(ctx context.Context, tenantID uuid.UUID, key string) (apiserver.StoredResponse, error) {
	resp, ok := s.entries[tenantID.String()+"|"+key]
	if !ok {
		return apiserver.StoredResponse{}, apiserver.ErrIdempotencyKeyNotFound
	}
	return resp, nil
}

func (s *spyStore) Save(ctx context.Context, tenantID uuid.UUID, key string, resp apiserver.StoredResponse) error {
	s.entries[tenantID.String()+"|"+key] = resp
	return nil
}

func TestWithIdempotency_SameKeySameBody_ReturnsCachedResponseWithoutReexecuting(t *testing.T) {
	store := newSpyStore()
	tenantID := uuid.New()
	callCount := 0

	handler := func(ctx context.Context) (*jenginev1.CreateAccountResponse, error) {
		callCount++
		return &jenginev1.CreateAccountResponse{Account: &jenginev1.Account{Id: "acct-1", Name: "First Call"}}, nil
	}
	newResp := func() *jenginev1.CreateAccountResponse { return &jenginev1.CreateAccountResponse{} }

	resp1, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-a", newResp, handler)
	if err != nil {
		t.Fatalf("first call failed: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected handler called once, got %d", callCount)
	}
	if resp1.Account.Id != "acct-1" {
		t.Fatalf("unexpected first response: %+v", resp1)
	}

	resp2, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-a", newResp, handler)
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}
	if callCount != 1 {
		t.Fatalf("expected handler NOT re-executed on cache hit, but call count is %d", callCount)
	}
	if resp2.Account.Id != "acct-1" {
		t.Fatalf("expected the cached response to be returned, got: %+v", resp2)
	}
}

func TestWithIdempotency_SameKeyDifferentBody_Rejected(t *testing.T) {
	store := newSpyStore()
	tenantID := uuid.New()

	handler := func(ctx context.Context) (*jenginev1.CreateAccountResponse, error) {
		return &jenginev1.CreateAccountResponse{Account: &jenginev1.Account{Id: "acct-1"}}, nil
	}
	newResp := func() *jenginev1.CreateAccountResponse { return &jenginev1.CreateAccountResponse{} }

	if _, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-a", newResp, handler); err != nil {
		t.Fatalf("first call failed: %v", err)
	}

	_, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-B-DIFFERENT", newResp, handler)
	if err == nil {
		t.Fatal("expected an error for the same key reused with a different request body")
	}
}

func TestWithIdempotency_MissingKey_Rejected(t *testing.T) {
	store := newSpyStore()
	tenantID := uuid.New()

	handler := func(ctx context.Context) (*jenginev1.CreateAccountResponse, error) {
		return &jenginev1.CreateAccountResponse{}, nil
	}
	newResp := func() *jenginev1.CreateAccountResponse { return &jenginev1.CreateAccountResponse{} }

	_, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "", "hash-a", newResp, handler)
	if err == nil {
		t.Fatal("expected an error when Idempotency-Key is missing")
	}
}

func TestWithIdempotency_HandlerErrorNotCached(t *testing.T) {
	store := newSpyStore()
	tenantID := uuid.New()
	callCount := 0

	wantErr := errors.New("boom")
	handler := func(ctx context.Context) (*jenginev1.CreateAccountResponse, error) {
		callCount++
		return nil, wantErr
	}
	newResp := func() *jenginev1.CreateAccountResponse { return &jenginev1.CreateAccountResponse{} }

	_, err := apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-a", newResp, handler)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected the handler's own error to propagate, got: %v", err)
	}

	// A retry with the same key must re-execute (the failed attempt was
	// never cached).
	_, _ = apiserver.WithIdempotency(context.Background(), store, tenantID, "key-1", "hash-a", newResp, handler)
	if callCount != 2 {
		t.Fatalf("expected the handler to be re-executed after a prior error, got callCount=%d", callCount)
	}
}
