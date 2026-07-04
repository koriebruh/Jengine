// Package audit implements the system-wide, append-only, hash-chained
// AuditEvent writer - the compliance-grade record that must survive even
// a full application-layer compromise (plans/docs/09-security-compliance.md
// §10.1). See plans/task/core/14.
package audit

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

// AuditEvent mirrors the audit_events table field-for-field
// (plans/docs/03-canonical-data-model.md §4.1). Hash is never persisted
// (the audit_events table has no hash column - HashChainPrev alone is
// enough information to re-derive it deterministically, see doc comment
// on ComputeHash) - it exists on this struct purely as the computed
// value passed to the next event's HashChainPrev.
type AuditEvent struct {
	ID            string // ULID, time-sortable
	TenantID      uuid.UUID
	ActorID       string
	ActorType     string // USER | SYSTEM | API_KEY
	EventType     string // e.g. "break.transitioned", "match.confirmed", "rule.activated"
	EntityType    string // e.g. "Break", "MatchResult", "MatchRule"
	EntityID      string
	BeforeState   json.RawMessage
	AfterState    json.RawMessage
	IPAddress     string
	RequestID     string
	OccurredAt    time.Time
	HashChainPrev string
	Hash          string
}

// NewULID generates a time-sortable, strictly-monotonic-within-the-same-
// millisecond ULID for AuditEvent.ID. Uses ulid.DefaultEntropy() - a
// single shared, mutex-guarded, crypto/rand-backed monotonic source - not
// a fresh ulid.Monotonic instance per call, which would silently defeat
// monotonicity (two ULIDs generated in the same millisecond could then
// sort equal or reversed, since the guarantee comes from carrying state
// across calls) and wouldn't be safe for concurrent callers (e.g.
// concurrent audit writers) either.
func NewULID() string {
	return ulid.MustNew(ulid.Timestamp(time.Now()), ulid.DefaultEntropy()).String()
}

// ComputeHash returns the deterministic SHA-256 hex digest of evt's
// payload fields plus prevHash (the previous event's own Hash in the
// same tenant's chain) - this becomes evt.Hash, which in turn becomes
// the *next* event's HashChainPrev.
//
// Canonicalization is explicit field concatenation (not a JSON encoder
// over a struct/map), since Go's map iteration order is randomized and
// even struct-to-JSON encoding order depends on the exact struct
// definition - either would make two logically-identical events hash
// differently between runs, making chain verification unreliable
// (plans/task/core/14 Common Pitfalls). BeforeState/AfterState are
// already deterministic json.RawMessage byte slices (whatever the caller
// passed in) - concatenated as-is, not re-marshaled.
func ComputeHash(evt AuditEvent, prevHash string) string {
	h := sha256.New()
	// hash.Hash.Write never returns an error (io.Writer contract note in
	// crypto/hash's doc) - errors discarded explicitly, not ignored by
	// accident.
	_, _ = fmt.Fprintf(h, "id=%s\ntenant_id=%s\nactor_id=%s\nactor_type=%s\nevent_type=%s\nentity_type=%s\nentity_id=%s\n",
		evt.ID, evt.TenantID, evt.ActorID, evt.ActorType, evt.EventType, evt.EntityType, evt.EntityID)
	_, _ = h.Write(evt.BeforeState)
	_, _ = h.Write([]byte{0}) // separator - avoids ambiguous concatenation between before/after
	_, _ = h.Write(evt.AfterState)
	_, _ = h.Write([]byte{0})
	_, _ = fmt.Fprintf(h, "ip_address=%s\nrequest_id=%s\noccurred_at=%s\nhash_chain_prev=%s\n",
		evt.IPAddress, evt.RequestID, evt.OccurredAt.UTC().Format(time.RFC3339Nano), prevHash)
	return hex.EncodeToString(h.Sum(nil))
}
