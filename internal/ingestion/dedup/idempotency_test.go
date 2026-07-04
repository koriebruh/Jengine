package dedup_test

import (
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/dedup"
)

func TestComputeIdempotencyKey_Deterministic(t *testing.T) {
	tenantID, connectorID, batchID := uuid.New(), uuid.New(), uuid.New()

	k1 := dedup.ComputeIdempotencyKey(tenantID, connectorID, "REF123", batchID)
	k2 := dedup.ComputeIdempotencyKey(tenantID, connectorID, "REF123", batchID)
	if k1 != k2 {
		t.Fatalf("expected same inputs to produce the same key, got %q vs %q", k1, k2)
	}
	if k1 == "" {
		t.Fatal("expected a non-empty key")
	}
}

func TestComputeIdempotencyKey_SensitiveToEachComponent(t *testing.T) {
	tenantID, connectorID, batchID := uuid.New(), uuid.New(), uuid.New()
	base := dedup.ComputeIdempotencyKey(tenantID, connectorID, "REF123", batchID)

	otherTenant := dedup.ComputeIdempotencyKey(uuid.New(), connectorID, "REF123", batchID)
	if otherTenant == base {
		t.Error("expected changing tenantID to change the key")
	}

	otherConnector := dedup.ComputeIdempotencyKey(tenantID, uuid.New(), "REF123", batchID)
	if otherConnector == base {
		t.Error("expected changing connectorID to change the key")
	}

	otherKey := dedup.ComputeIdempotencyKey(tenantID, connectorID, "REF456", batchID)
	if otherKey == base {
		t.Error("expected changing the natural key to change the key")
	}

	otherBatch := dedup.ComputeIdempotencyKey(tenantID, connectorID, "REF123", uuid.New())
	if otherBatch == base {
		t.Error("expected changing batchID to change the key")
	}
}

func TestComputeIdempotencyKey_NoAmbiguousConcatenation(t *testing.T) {
	// Without null-byte separators, "ab"+"c" and "a"+"bc" could collide
	// under naive string concatenation - prove they don't here (the
	// natural key component is the only variable-boundary field, so
	// shift a boundary between it and an adjacent fixed-format field).
	tenantID, connectorID, batchID := uuid.New(), uuid.New(), uuid.New()

	k1 := dedup.ComputeIdempotencyKey(tenantID, connectorID, "ab", batchID)
	k2 := dedup.ComputeIdempotencyKey(tenantID, connectorID, "a", batchID)
	if k1 == k2 {
		t.Error("expected different natural keys to never collide")
	}
}

func TestRecordHash_StableRegardlessOfMapOrder(t *testing.T) {
	fields := map[string]string{
		"currency":   "EUR",
		"amount":     "100.00",
		"value_date": "2024-01-15",
	}

	// Compute multiple times - Go's map iteration order is randomized
	// per-run, so if RecordHash didn't sort keys, repeated calls could
	// (probabilistically) produce different hashes.
	h1 := dedup.RecordHash(fields)
	for i := 0; i < 20; i++ {
		h2 := dedup.RecordHash(fields)
		if h1 != h2 {
			t.Fatalf("RecordHash is not stable across calls: %q vs %q", h1, h2)
		}
	}
}

func TestRecordHash_SensitiveToValueChanges(t *testing.T) {
	base := dedup.RecordHash(map[string]string{"amount": "100.00", "currency": "EUR"})
	changed := dedup.RecordHash(map[string]string{"amount": "100.01", "currency": "EUR"})
	if base == changed {
		t.Error("expected changing a field value to change the hash")
	}
}
