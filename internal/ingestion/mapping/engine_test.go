package mapping_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/mapping"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
)

type fakeSpecLookup struct {
	specBytes []byte
	err       error
}

func (f *fakeSpecLookup) GetActive(ctx context.Context, tenantID uuid.UUID, sourceFormat string) (domain.MappingSpec, error) {
	if f.err != nil {
		return domain.MappingSpec{}, f.err
	}
	return domain.MappingSpec{SourceFormat: sourceFormat, Version: 1, Status: domain.MappingSpecStatusActive, Spec: f.specBytes}, nil
}

// passthroughTxRunner just calls fn directly - fine for tests since
// fakeSpecLookup doesn't need a real ambient transaction.
func passthroughTxRunner(ctx context.Context, tenantID uuid.UUID, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

func loadFixture(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture %s: %v", path, err)
	}
	return b
}

func newMT940ParsedRecord(tenantID uuid.UUID) *pipeline.PipelineRecord {
	return &pipeline.PipelineRecord{
		Raw: connector.RawRecord{TenantID: tenantID, SourceFormat: "MT940"},
		ParsedFields: map[string]any{
			"field_61": map[string]any{
				"amount":            "250,00",
				"debit_credit_mark": "D",
				"currency":          "eur",
				"value_date":        "240102",
			},
			"field_86": map[string]any{
				"narrative": "some narrative REF:ABC123 trailing text",
			},
		},
	}
}

func TestMappingEngine_FullExampleYAML_ProducesCorrectMappedFields(t *testing.T) {
	specBytes := loadFixture(t, "testdata/mt940_default.yaml")
	engine := mapping.NewEngine(&fakeSpecLookup{specBytes: specBytes}, passthroughTxRunner)

	tenantID := uuid.New()
	rec := newMT940ParsedRecord(tenantID)

	result, err := engine.Process(context.Background(), rec)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if result != pipeline.StageContinue {
		t.Fatalf("expected StageContinue, got %v", result)
	}

	amount, ok := rec.MappedFields["transaction.amount"].(decimal.Decimal)
	if !ok {
		t.Fatalf("transaction.amount missing or wrong type: %+v", rec.MappedFields)
	}
	if !amount.Equal(decimal.RequireFromString("-250.00")) {
		t.Errorf("expected -250.00 (debit), got %s", amount)
	}

	currency, ok := rec.MappedFields["transaction.currency"].(string)
	if !ok || currency != "EUR" {
		t.Errorf("expected currency EUR, got %v", rec.MappedFields["transaction.currency"])
	}

	valueDate, ok := rec.MappedFields["transaction.value_date"].(time.Time)
	if !ok || valueDate.Year() != 2024 || valueDate.Month() != 1 || valueDate.Day() != 2 {
		t.Errorf("unexpected value_date: %v", rec.MappedFields["transaction.value_date"])
	}

	reference, ok := rec.MappedFields["transaction.reference"].(string)
	if !ok || reference != "ABC123" {
		t.Errorf("expected reference ABC123, got %v", rec.MappedFields["transaction.reference"])
	}
}

func TestMappingEngine_TransformFailure_QuarantinesWithReasonNamingField(t *testing.T) {
	specBytes := loadFixture(t, "testdata/mt940_default.yaml")
	engine := mapping.NewEngine(&fakeSpecLookup{specBytes: specBytes}, passthroughTxRunner)

	tenantID := uuid.New()
	rec := newMT940ParsedRecord(tenantID)
	// Corrupt the date field so parse_date("YYMMDD") fails.
	rec.ParsedFields["field_61"].(map[string]any)["value_date"] = "not-a-date"

	result, err := engine.Process(context.Background(), rec)
	if result != pipeline.StageQuarantine {
		t.Fatalf("expected StageQuarantine, got %v (err=%v)", result, err)
	}
	if err == nil {
		t.Fatal("expected a non-nil error naming the failing field")
	}
	if !contains(err.Error(), "transaction.value_date") {
		t.Errorf("expected error to name the failing target field transaction.value_date, got: %v", err)
	}
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestMappingEngine_CSVFixture(t *testing.T) {
	specBytes := loadFixture(t, "testdata/csv_default.yaml")
	engine := mapping.NewEngine(&fakeSpecLookup{specBytes: specBytes}, passthroughTxRunner)

	tenantID := uuid.New()
	rec := &pipeline.PipelineRecord{
		Raw: connector.RawRecord{TenantID: tenantID, SourceFormat: "CSV"},
		ParsedFields: map[string]any{
			"amount":      "42.50",
			"currency":    "usd",
			"value_date":  "2024-03-15",
			"description": "  office supplies  ",
		},
	}

	result, err := engine.Process(context.Background(), rec)
	if err != nil {
		t.Fatalf("Process failed: %v", err)
	}
	if result != pipeline.StageContinue {
		t.Fatalf("expected StageContinue, got %v", result)
	}
	if amount := rec.MappedFields["transaction.amount"].(decimal.Decimal); !amount.Equal(decimal.RequireFromString("42.50")) {
		t.Errorf("unexpected amount: %s", amount)
	}
	if desc := rec.MappedFields["transaction.description"].(string); desc != "office supplies" {
		t.Errorf("unexpected description: %q", desc)
	}
}
