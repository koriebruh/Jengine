// Package apiserver implements plans/task/core/15's Connect-RPC service
// handlers - the MVP public API surface backing the frontend and any
// early API-integrating design partner.
package apiserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// ErrIdempotencyKeyNotFound is returned by IdempotencyStore.Get when no
// stored response exists for (tenantID, key) - a cache miss, not an
// error condition callers need to log.
var ErrIdempotencyKeyNotFound = errors.New("apiserver: idempotency key not found")

// StoredResponse is what IdempotencyStore persists per (tenant,
// idempotency key): the successful response body (proto-marshaled
// bytes) plus the request hash that produced it, so a later call with
// the SAME key but a DIFFERENT body can be rejected rather than
// ambiguously replayed or silently re-executed (plans/task/core/15
// Implementation Notes, step 3).
type StoredResponse struct {
	RequestHash  string
	ResponseBody []byte
}

// IdempotencyStore is the Idempotency-Key backing store.
type IdempotencyStore interface {
	Get(ctx context.Context, tenantID uuid.UUID, key string) (StoredResponse, error)
	Save(ctx context.Context, tenantID uuid.UUID, key string, resp StoredResponse) error
}

// ComputeRequestHash hashes method + tenant + request body - identical
// (tenant, key, method, body) tuples always hash identically, so a
// retried request with the same key and body is recognized as the same
// logical request (plans/task/core/15 Implementation Notes, step 2).
func ComputeRequestHash(tenantID uuid.UUID, method string, req proto.Message) (string, error) {
	body, err := proto.Marshal(req)
	if err != nil {
		return "", fmt.Errorf("apiserver: marshal request for idempotency hash: %w", err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(tenantID.String()))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(method))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(body)
	return hex.EncodeToString(h.Sum(nil)), nil
}

// WithIdempotency wraps a mutating handler with Idempotency-Key
// semantics (plans/task/core/15 Implementation Notes):
//  1. missing key -> rejected (required on every mutating RPC for MVP
//     simplicity - one rule, not a per-endpoint policy to remember).
//  2. same key + same request body (by hash) -> the ORIGINAL stored
//     response is returned, handler is NOT re-executed.
//  3. same key + different request body -> rejected as a client bug
//     (key reuse across different requests), never silently executed
//     either version.
//  4. new key -> handler executes once, its response is cached for
//     future replays.
//
// This is a generic helper rather than a connect.Interceptor operating
// on type-erased connect.AnyRequest/AnyResponse: replaying a cached
// response generically at that layer would require reconstructing an
// arbitrary proto.Message via reflection/dynamicpb with no compile-time
// type information, which is real complexity for no behavioral
// difference - every mutating handler in this package calls this helper
// directly instead, with its own concrete Req/Resp types already in
// scope, achieving the identical requirement (functionally the same
// header-driven cache-or-execute-once behavior, plans/task/core/15's own
// wording: "Interceptor logic for mutating RPCs") more simply.
func WithIdempotency[Resp proto.Message](
	ctx context.Context,
	store IdempotencyStore,
	tenantID uuid.UUID,
	idempotencyKey string,
	requestHash string,
	newResp func() Resp,
	handler func(ctx context.Context) (Resp, error),
) (Resp, error) {
	var zero Resp

	if idempotencyKey == "" {
		return zero, connect.NewError(connect.CodeInvalidArgument, errors.New("apiserver: Idempotency-Key header is required for mutating requests"))
	}

	stored, err := store.Get(ctx, tenantID, idempotencyKey)
	if err != nil && !errors.Is(err, ErrIdempotencyKeyNotFound) {
		return zero, err
	}
	if err == nil {
		if stored.RequestHash != requestHash {
			return zero, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("apiserver: idempotency key %q was already used for a different request", idempotencyKey))
		}
		resp := newResp()
		if err := proto.Unmarshal(stored.ResponseBody, resp); err != nil {
			return zero, fmt.Errorf("apiserver: unmarshal cached idempotent response: %w", err)
		}
		return resp, nil
	}

	resp, err := handler(ctx)
	if err != nil {
		// Only successful responses are cached - an error means the
		// underlying operation didn't durably happen (or failed
		// validation before doing anything), so a retry with the same
		// key should genuinely re-attempt it, not replay the error.
		return resp, err
	}

	body, marshalErr := proto.Marshal(resp)
	if marshalErr == nil {
		// Best-effort cache write: the underlying operation already
		// succeeded, so a save failure here shouldn't fail the request
		// that already completed - it just means a retry with this key
		// re-executes rather than replaying (unlike task 14's audit
		// writes, this isn't a zero-loss-tolerance guarantee).
		_ = store.Save(ctx, tenantID, idempotencyKey, StoredResponse{RequestHash: requestHash, ResponseBody: body})
	}
	return resp, nil
}
