package webhookreceiver_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/connector/webhookreceiver"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

type fakeSecrets struct{ secret string }

func (f fakeSecrets) Resolve(ctx context.Context, ref string) (string, error) {
	return f.secret, nil
}

func sign(secret string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func setup(t *testing.T) (c *webhookreceiver.Connector, tenantID, connectorID uuid.UUID, secret string, records <-chan connector.RawRecord) {
	t.Helper()
	c = webhookreceiver.New(fakeSecrets{secret: "topsecret"})
	tenantID, connectorID = uuid.New(), uuid.New()
	cfg := connector.ConnectorConfig{
		TenantID: tenantID, ConnectorID: connectorID, Type: "WEBHOOK_RECEIVER",
		Settings: mustJSON(webhookreceiver.Config{
			HMACSecretRef:    "secret/data/webhook",
			SignatureHeader:  "X-Signature",
			SignatureScheme:  webhookreceiver.SchemeGenericHMACSHA256,
			DeliveryIDHeader: "X-Delivery-Id",
			SourceFormat:     "GENERIC_WEBHOOK_JSON",
		}),
	}
	records, err := c.Fetch(context.Background(), cfg)
	if err != nil {
		t.Fatalf("Fetch (registration) failed: %v", err)
	}
	return c, tenantID, connectorID, "topsecret", records
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

func TestServeHTTP_ValidSignature_EnqueuesRecord(t *testing.T) {
	c, tenantID, connectorID, secret, records := setup(t)

	body := []byte(`{"event":"settlement.created","id":"evt_1"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/ingest/x/y", strings.NewReader(string(body)))
	req.Header.Set("X-Signature", sign(secret, body))
	req.Header.Set("X-Delivery-Id", "delivery-1")
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req, tenantID, connectorID)

	if rw.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rw.Code, rw.Body.String())
	}

	select {
	case rec := <-records:
		if rec.TenantID != tenantID || rec.ConnectorID != connectorID {
			t.Errorf("unexpected record identity: %+v", rec)
		}
		if string(rec.Payload) != string(body) {
			t.Errorf("payload mismatch: got %q want %q", rec.Payload, body)
		}
		if rec.SourceFormat != "GENERIC_WEBHOOK_JSON" {
			t.Errorf("unexpected SourceFormat: %q", rec.SourceFormat)
		}
	default:
		t.Fatal("expected a record to be enqueued")
	}
}

func TestServeHTTP_InvalidSignature_Rejected(t *testing.T) {
	c, tenantID, connectorID, _, records := setup(t)

	body := []byte(`{"event":"settlement.created"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/ingest/x/y", strings.NewReader(string(body)))
	req.Header.Set("X-Signature", "0000000000000000000000000000000000000000000000000000000000000000")
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req, tenantID, connectorID)

	if rw.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rw.Code)
	}
	select {
	case rec := <-records:
		t.Fatalf("expected no record enqueued for invalid signature, got %+v", rec)
	default:
	}
}

func TestServeHTTP_ReplayedDelivery_NoOpNotDoubleEnqueued(t *testing.T) {
	c, tenantID, connectorID, secret, records := setup(t)

	body := []byte(`{"event":"settlement.created","id":"evt_replay"}`)
	sig := sign(secret, body)

	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/ingest/x/y", strings.NewReader(string(body)))
		req.Header.Set("X-Signature", sig)
		req.Header.Set("X-Delivery-Id", "delivery-replay")
		rw := httptest.NewRecorder()
		c.ServeHTTP(rw, req, tenantID, connectorID)
		if rw.Code != http.StatusOK {
			t.Fatalf("attempt %d: expected 200, got %d", i, rw.Code)
		}
	}

	count := 0
drain:
	for {
		select {
		case <-records:
			count++
		default:
			break drain
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 enqueued record for a replayed delivery, got %d", count)
	}
}

func TestServeHTTP_UnknownConnector_404(t *testing.T) {
	c, tenantID, _, secret, _ := setup(t)
	body := []byte(`{}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/ingest/x/y", strings.NewReader(string(body)))
	req.Header.Set("X-Signature", sign(secret, body))
	rw := httptest.NewRecorder()

	c.ServeHTTP(rw, req, tenantID, uuid.New())

	if rw.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rw.Code)
	}
}

func TestValidate_RejectsMissingFields(t *testing.T) {
	c := webhookreceiver.New(fakeSecrets{secret: "x"})
	cfg := connector.ConnectorConfig{Settings: mustJSON(webhookreceiver.Config{})}
	if err := c.Validate(cfg); err == nil {
		t.Fatal("expected Validate to reject empty config")
	}
}

func TestNaturalKeyFunc_StableForSameBody(t *testing.T) {
	recWithPayload := func(body []byte) *pipeline.PipelineRecord {
		return &pipeline.PipelineRecord{Raw: connector.RawRecord{Payload: body}}
	}
	rec1 := recWithPayload([]byte("same-body"))
	rec2 := recWithPayload([]byte("same-body"))
	if webhookreceiver.NaturalKeyFunc(rec1) != webhookreceiver.NaturalKeyFunc(rec2) {
		t.Error("expected same body to produce same natural key")
	}
	rec3 := recWithPayload([]byte("different-body"))
	if webhookreceiver.NaturalKeyFunc(rec1) == webhookreceiver.NaturalKeyFunc(rec3) {
		t.Error("expected different body to produce different natural key")
	}
}
