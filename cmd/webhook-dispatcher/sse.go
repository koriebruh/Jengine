package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/notify"
)

// sseEvent is what gets pushed to a connected browser session -
// plans/task/core/21's SSE gateway subsection: "reuse the same event-
// catalog types this task already defines... the SSE gateway and the
// webhook dispatcher are two delivery mechanisms for one shared event
// model, not two event models."
type sseEvent struct {
	EventType string          `json:"event_type"`
	Payload   json.RawMessage `json:"payload"`
}

// Hub fans out case-event-topic records to connected per-tenant SSE
// clients - a second, lightweight consumer of the SAME
// processRecord/Kafka-consume-loop this file's sibling (consume.go)
// already reads for webhook delivery, per this task's own ownership-
// assignment rationale ("a second, small consumer sharing that
// infrastructure, not a new subsystem"). No HMAC signing, no retry/DLQ -
// an SSE client that misses an event re-syncs via the frontend's own
// REST fallback poll, per this task's explicit Implementation Notes.
type Hub struct {
	mu   sync.Mutex
	subs map[uuid.UUID]map[chan sseEvent]struct{}
}

func NewHub() *Hub {
	return &Hub{subs: make(map[uuid.UUID]map[chan sseEvent]struct{})}
}

func (h *Hub) Subscribe(tenantID uuid.UUID) chan sseEvent {
	ch := make(chan sseEvent, 32)
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.subs[tenantID] == nil {
		h.subs[tenantID] = make(map[chan sseEvent]struct{})
	}
	h.subs[tenantID][ch] = struct{}{}
	return ch
}

func (h *Hub) Unsubscribe(tenantID uuid.UUID, ch chan sseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.subs[tenantID], ch)
	close(ch)
}

// Broadcast fans evt out to every currently-connected subscriber for
// tenantID - non-blocking per subscriber (a slow/stuck browser session
// drops events rather than blocking the shared Kafka consume loop every
// other tenant/subscriber depends on).
func (h *Hub) Broadcast(tenantID uuid.UUID, evt sseEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[tenantID] {
		select {
		case ch <- evt:
		default:
		}
	}
}

// streamHandler serves GET /v1/tenants/{tenant_id}/events/stream -
// plans/task/core/21's SSE gateway subsection. Auth via a short-lived
// stream-token query parameter (browser EventSource can't set an
// Authorization header) rather than the bearer mechanism task 15's
// Connect-RPC surface uses - minted via WebhookService.MintStreamToken,
// verified here without a DB round-trip (internal/notify.VerifyStreamToken).
func streamHandler(hub *Hub, streamTokenSecret string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pathTenantID, err := uuid.Parse(r.PathValue("tenant_id"))
		if err != nil {
			http.Error(w, "invalid tenant_id", http.StatusBadRequest)
			return
		}
		token := r.URL.Query().Get("token")
		tokenTenantID, err := notify.VerifyStreamToken(streamTokenSecret, token, time.Now())
		if err != nil {
			http.Error(w, "invalid or expired stream token", http.StatusUnauthorized)
			return
		}
		if tokenTenantID != pathTenantID {
			http.Error(w, "token does not authorize this tenant", http.StatusForbidden)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "streaming unsupported", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ch := hub.Subscribe(pathTenantID)
		defer hub.Unsubscribe(pathTenantID, ch)

		ctx := r.Context()
		for {
			select {
			case <-ctx.Done():
				return
			case evt, ok := <-ch:
				if !ok {
					return
				}
				data, err := json.Marshal(evt)
				if err != nil {
					continue
				}
				if _, err := fmt.Fprintf(w, "event: %s\ndata: %s\n\n", evt.EventType, data); err != nil {
					return // client disconnected
				}
				flusher.Flush()
			}
		}
	}
}
