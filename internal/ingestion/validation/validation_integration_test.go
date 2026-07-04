package validation_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
	"github.com/koriebruh/Jengine/internal/ingestion/pipeline"
	"github.com/koriebruh/Jengine/internal/ingestion/validation"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

// TestValidationStage_SchemaFailure_RoutesToQuarantine proves
// plans/task/core/09's Definition of Done: schema validation rejects a
// record missing a required field and routes it to a real, queryable
// quarantine_entries row with a reason mentioning the missing field.
func TestValidationStage_SchemaFailure_RoutesToQuarantine(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	connectorID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'test', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		t.Fatalf("seed connector failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	quarantineSink := postgres.NewPipelineQuarantineSink(db.Pool)

	// A record missing currency entirely - Normalized.Currency stays "".
	missingCurrencyStage := stageSettingNormalized(pipeline.NormalizedFields{
		ValueDate: time.Now(),
		Side:      "DEBIT",
		// Currency deliberately left empty.
	})

	pl := &pipeline.Pipeline{
		Stages:     []pipeline.Stage{missingCurrencyStage, &validation.ValidationStage{}},
		Quarantine: quarantineSink,
	}

	rec := connector.RawRecord{TenantID: tenantID, ConnectorID: connectorID, BatchID: uuid.New(), Payload: []byte("test-payload")}
	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, singleRecordConnector{rec}, connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}); err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}

	var reason string
	if err := db.Pool.QueryRow(ctx,
		`SELECT reason FROM quarantine_entries WHERE tenant_id = $1 AND connector_id = $2`,
		tenantID, connectorID,
	).Scan(&reason); err != nil {
		t.Fatalf("expected a quarantine_entries row, query failed: %v", err)
	}
	if !containsSubstr(reason, "currency") {
		t.Errorf("expected quarantine reason to mention currency, got %q", reason)
	}
}

// TestValidationStage_BusinessRuleFailure_RoutesToQuarantine proves the
// business-validation half of the same DoD point: a transaction against
// a non-allowlisted account is quarantined with a reason naming the
// account_allowlist rule.
func TestValidationStage_BusinessRuleFailure_RoutesToQuarantine(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	connectorID := uuid.New()
	if _, err := db.Pool.Exec(ctx,
		`INSERT INTO connectors (id, tenant_id, type, status) VALUES ($1, $2, 'test', 'ACTIVE')`,
		connectorID, tenantID,
	); err != nil {
		t.Fatalf("seed connector failed: %v", err)
	}

	appPool := testutil.AppRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	quarantineSink := postgres.NewPipelineQuarantineSink(db.Pool)

	validRecordStage := stageSettingNormalized(pipeline.NormalizedFields{
		Currency: "EUR", ValueDate: time.Now(), Side: "DEBIT",
	})

	notAllowedAccountID := uuid.New()
	allowlistRule, _ := json.Marshal(map[string]any{"rule": "account_allowlist", "allowed_account_ids": []uuid.UUID{uuid.New()}})

	pl := &pipeline.Pipeline{
		Stages: []pipeline.Stage{
			validRecordStage,
			&validation.ValidationStage{
				AccountID:         notAllowedAccountID,
				BusinessValidator: &validation.BusinessValidator{Rules: []json.RawMessage{allowlistRule}},
			},
		},
		Quarantine: quarantineSink,
	}

	rec := connector.RawRecord{TenantID: tenantID, ConnectorID: connectorID, BatchID: uuid.New(), Payload: []byte("test-payload")}
	runCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	if err := pl.Run(runCtx, singleRecordConnector{rec}, connector.ConnectorConfig{TenantID: tenantID, ConnectorID: connectorID}); err != nil {
		t.Fatalf("pipeline run failed: %v", err)
	}

	var reason string
	if err := db.Pool.QueryRow(ctx,
		`SELECT reason FROM quarantine_entries WHERE tenant_id = $1 AND connector_id = $2`,
		tenantID, connectorID,
	).Scan(&reason); err != nil {
		t.Fatalf("expected a quarantine_entries row, query failed: %v", err)
	}
	if !containsSubstr(reason, "account_allowlist") {
		t.Errorf("expected quarantine reason to name the account_allowlist rule, got %q", reason)
	}
}

type setNormalizedStage struct{ fields pipeline.NormalizedFields }

func (s setNormalizedStage) Name() string { return "test_normalize" }
func (s setNormalizedStage) Process(ctx context.Context, rec *pipeline.PipelineRecord) (pipeline.StageResult, error) {
	rec.Normalized = s.fields
	return pipeline.StageContinue, nil
}

func stageSettingNormalized(fields pipeline.NormalizedFields) pipeline.Stage {
	return setNormalizedStage{fields: fields}
}

type singleRecordConnector struct {
	rec connector.RawRecord
}

func (c singleRecordConnector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	ch := make(chan connector.RawRecord, 1)
	ch <- c.rec
	close(ch)
	return ch, nil
}
func (c singleRecordConnector) Validate(cfg connector.ConnectorConfig) error { return nil }
func (c singleRecordConnector) SupportsStreaming() bool                      { return false }
func (c singleRecordConnector) Checkpoint() (connector.Cursor, error)        { return connector.Cursor{}, nil }

func containsSubstr(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
