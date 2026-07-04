package audit_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/audit"
)

func sampleEvent() audit.AuditEvent {
	return audit.AuditEvent{
		ID: "01HZY000000000000000000000", TenantID: uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		ActorID: "user-1", ActorType: "USER", EventType: "break.transitioned",
		EntityType: "Break", EntityID: "break-1",
		BeforeState: json.RawMessage(`{"status":"OPEN"}`), AfterState: json.RawMessage(`{"status":"ASSIGNED"}`),
		IPAddress: "127.0.0.1", RequestID: "req-1",
		OccurredAt: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

func TestComputeHash_Deterministic(t *testing.T) {
	evt := sampleEvent()
	h1 := audit.ComputeHash(evt, "prevhash")
	h2 := audit.ComputeHash(evt, "prevhash")
	if h1 != h2 {
		t.Fatalf("expected identical hash for identical input, got %q vs %q", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("expected a 64-char hex SHA-256 digest, got len=%d: %q", len(h1), h1)
	}
}

func TestComputeHash_SingleFieldChangeAltersHash(t *testing.T) {
	base := sampleEvent()
	baseHash := audit.ComputeHash(base, "prevhash")

	cases := []struct {
		name   string
		modify func(*audit.AuditEvent)
	}{
		{"actor_id", func(e *audit.AuditEvent) { e.ActorID = "user-2" }},
		{"event_type", func(e *audit.AuditEvent) { e.EventType = "break.resolved" }},
		{"entity_id", func(e *audit.AuditEvent) { e.EntityID = "break-2" }},
		{"before_state", func(e *audit.AuditEvent) { e.BeforeState = json.RawMessage(`{"status":"DIFFERENT"}`) }},
		{"after_state", func(e *audit.AuditEvent) { e.AfterState = json.RawMessage(`{"status":"DIFFERENT"}`) }},
		{"ip_address", func(e *audit.AuditEvent) { e.IPAddress = "10.0.0.1" }},
		{"request_id", func(e *audit.AuditEvent) { e.RequestID = "req-2" }},
		{"occurred_at", func(e *audit.AuditEvent) { e.OccurredAt = e.OccurredAt.Add(time.Second) }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			modified := sampleEvent()
			c.modify(&modified)
			gotHash := audit.ComputeHash(modified, "prevhash")
			if gotHash == baseHash {
				t.Errorf("expected changing %s to alter the hash, but it stayed %q", c.name, gotHash)
			}
		})
	}
}

func TestComputeHash_DifferentPrevHashAltersHash(t *testing.T) {
	evt := sampleEvent()
	h1 := audit.ComputeHash(evt, "prevhash-a")
	h2 := audit.ComputeHash(evt, "prevhash-b")
	if h1 == h2 {
		t.Fatal("expected different prevHash to produce different hash")
	}
}

func TestNewULID_MonotonicallyIncreasing(t *testing.T) {
	a := audit.NewULID()
	b := audit.NewULID()
	if a >= b {
		t.Errorf("expected successive ULIDs to be strictly increasing, got %q then %q", a, b)
	}
}
