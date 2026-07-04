package dedup

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/google/uuid"
)

// ComputeIdempotencyKey derives the key stored in both
// Transaction.ingestion_idempotency_key and ingestion_dedup.idempotency_key
// (plans/docs/02-data-ingestion.md §3.4). Null-byte separators between
// components avoid ambiguous concatenation collisions (e.g. "ab"+"c" vs
// "a"+"bc" would hash identically without them).
func ComputeIdempotencyKey(tenantID, connectorID uuid.UUID, naturalKeyOrRecordHash string, batchID uuid.UUID) string {
	h := sha256.New()
	h.Write([]byte(tenantID.String()))
	h.Write([]byte{0})
	h.Write([]byte(connectorID.String()))
	h.Write([]byte{0})
	h.Write([]byte(naturalKeyOrRecordHash))
	h.Write([]byte{0})
	h.Write([]byte(batchID.String()))
	return hex.EncodeToString(h.Sum(nil))
}

// RecordHash is the fallback natural-key input when a source format
// doesn't provide its own reliable natural key (plans/task/core/09
// Implementation Notes) - a deterministic hash of a record's stable
// fields. Map key iteration order in Go is randomized, so keys are
// sorted before hashing to keep the result stable across runs
// (plans/task/core/09 DoD explicitly requires this).
func RecordHash(fields map[string]string) string {
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte(0)
		sb.WriteString(fields[k])
		sb.WriteByte(0)
	}

	sum := sha256.Sum256([]byte(sb.String()))
	return hex.EncodeToString(sum[:])
}
