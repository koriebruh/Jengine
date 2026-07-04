package domain

import (
	"encoding/json"
	"net"
	"time"

	"github.com/google/uuid"
)

// AuditEvent mirrors the audit_events table. ID is a ULID (26-char,
// time-sortable, generated application-side), stored as text/string, not
// uuid.UUID - see migrations/0001_init_schema.up.sql. HashChainPrev
// linking logic is plans/task/core/14, not this task - this struct only
// carries the field.
type AuditEvent struct {
	ID            string // ULID
	TenantID      uuid.UUID
	ActorID       *string
	ActorType     string
	EventType     string
	EntityType    string
	EntityID      string
	BeforeState   json.RawMessage
	AfterState    json.RawMessage
	IPAddress     net.IP
	RequestID     *string
	OccurredAt    time.Time
	HashChainPrev *string
}
