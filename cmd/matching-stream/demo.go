package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	jenginev1 "github.com/koriebruh/Jengine/gen/go/jengine/v1"
	"github.com/koriebruh/Jengine/internal/matching/rules"
)

// publishTestEvent is the manual-verification helper plans/task/core/19's
// Definition of Done asks for: seeds a fresh tenant/account/streaming-
// enabled rule and two matching transactions, then publishes both as
// TransactionEvent messages onto normalized.transactions.default -
// enough for a running `matching-stream -demo=serve` process to observe
// a provisional AUTO_MATCHED_STREAMING result end-to-end. Prints the
// seeded tenant/transaction IDs so the operator can verify the resulting
// match_results row afterward.
//
// This is also the concrete evidence for QA_REPORT.md's noted gap: no
// ingestion-pipeline stage currently PRODUCES real TransactionEvents
// onto this topic (tasks 06-09 persist to Postgres directly, no Kafka
// publish step exists yet) - this demo mode is what proves
// matching-stream's own consumption side works correctly against the
// schema task 18 defined, independent of that gap.
func publishTestEvent(ctx context.Context, brokers []string) error {
	superuserDSN := envOrDefault("DATABASE_URL", "postgres://jengine:jengine_dev@localhost:5432/jengine?sslmode=disable")
	pool, err := pgxpool.New(ctx, superuserDSN)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer pool.Close()

	tenantID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'matching-stream demo', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	); err != nil {
		return fmt.Errorf("seed tenant: %w", err)
	}

	accountA, accountB := uuid.New(), uuid.New()
	for _, id := range []uuid.UUID{accountA, accountB} {
		if _, err := pool.Exec(ctx,
			`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, $3, 'BANK', 'USD', 'Demo Account')`,
			id, tenantID, id.String(),
		); err != nil {
			return fmt.Errorf("seed account: %w", err)
		}
	}

	ruleSpec := rules.RuleSpec{}
	ruleSpec.Rule.Name = "matching-stream demo rule"
	ruleSpec.Rule.Version = 1
	ruleSpec.Rule.MatchCardinality = "ONE_TO_ONE"
	ruleSpec.Rule.Keys = []rules.KeySpec{{Field: "currency", Tolerance: rules.ToleranceYAML{Type: "exact"}}}
	ruleSpec.Rule.Scoring = []rules.ScoringSpec{{Field: "reference", Method: "exact", Weight: 1.0}}
	ruleSpec.Rule.Thresholds = rules.ThresholdSpec{AutoMatch: 0.9, Suggest: 0.5}
	ruleSpec.Rule.Execution = rules.ExecutionSpec{Priority: 1, Mode: []string{"streaming"}}
	ruleSpecJSON, err := json.Marshal(ruleSpec)
	if err != nil {
		return fmt.Errorf("marshal rule spec: %w", err)
	}
	ruleID := uuid.New()
	if _, err := pool.Exec(ctx,
		`INSERT INTO match_rules (id, tenant_id, name, version, status, rule_spec, match_type, source_account_id, target_account_id, priority, auto_match_threshold, created_by)
		 VALUES ($1, $2, 'matching-stream demo rule', 1, 'ACTIVE', $3, 'COMPOSITE', $4, $5, 1, 0.9, 'demo')`,
		ruleID, tenantID, ruleSpecJSON, accountA, accountB,
	); err != nil {
		return fmt.Errorf("seed match rule: %w", err)
	}

	day := time.Now()
	insertTx := func(accountID uuid.UUID, ref string) uuid.UUID {
		id := uuid.New()
		amount := decimal.NewFromInt(100)
		_, _ = pool.Exec(ctx,
			`INSERT INTO transactions (id, tenant_id, account_id, external_ref, amount, currency, base_amount, value_date, booking_date, side, source_mode, ingestion_idempotency_key, status)
			 VALUES ($1, $2, $3, $4, $5, 'USD', $5, $6, $6, 'DEBIT', 'STREAM', $7, 'UNMATCHED')`,
			id, tenantID, accountID, ref, amount, day, id.String(),
		)
		return id
	}
	firstTxID := insertTx(accountA, "REF-DEMO-001")
	secondTxID := insertTx(accountB, "REF-DEMO-001")

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("new kafka client: %w", err)
	}
	defer client.Close()

	publish := func(txID, accountID uuid.UUID) error {
		evt := &jenginev1.TransactionEvent{
			TenantId:      tenantID.String(),
			TransactionId: txID.String(),
			AccountId:     accountID.String(),
			ValueDate:     timestamppb.New(day),
			Amount:        &jenginev1.Money{Units: 100, CurrencyCode: "USD"},
			ExternalRef:   "REF-DEMO-001",
			SourceMode:    jenginev1.SourceMode_SOURCE_MODE_STREAM,
		}
		payload, err := proto.Marshal(evt)
		if err != nil {
			return fmt.Errorf("marshal TransactionEvent: %w", err)
		}
		res := client.ProduceSync(ctx, &kgo.Record{Topic: streamTopic, Key: []byte(tenantID.String()), Value: payload})
		return res.FirstErr()
	}

	if err := publish(firstTxID, accountA); err != nil {
		return fmt.Errorf("publish first event: %w", err)
	}
	if err := publish(secondTxID, accountB); err != nil {
		return fmt.Errorf("publish second event: %w", err)
	}

	fmt.Printf("published 2 TransactionEvents to %s\n", streamTopic)
	fmt.Printf("tenant_id=%s\n", tenantID)
	fmt.Printf("first_transaction_id=%s (account=%s)\n", firstTxID, accountA)
	fmt.Printf("second_transaction_id=%s (account=%s)\n", secondTxID, accountB)
	fmt.Println("run `matching-stream` (default -demo=serve) to consume these, then check:")
	fmt.Printf("  SELECT status FROM match_results WHERE tenant_id = '%s';\n", tenantID)
	return nil
}
