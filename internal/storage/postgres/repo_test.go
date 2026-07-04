package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/storage/postgres"
	"github.com/koriebruh/Jengine/internal/tenancy"
	"github.com/koriebruh/Jengine/internal/testutil"
)

func TestRepositories_CRUD(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}

	appPool := appRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})

	accountRepo := postgres.NewAccountRepo()
	statementRepo := postgres.NewStatementRepo()
	txRepo := postgres.NewTransactionRepo()
	ruleRepo := postgres.NewMatchRuleRepo()
	resultRepo := postgres.NewMatchResultRepo()
	caseRepo := postgres.NewCaseRepo()
	connectorRepo := postgres.NewConnectorRepo()

	var account domain.Account
	var statement domain.Statement
	var transaction domain.Transaction
	var rule domain.MatchRule
	var caseRow domain.Case
	var connector domain.Connector

	t.Run("AccountRepo", func(t *testing.T) {
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			account, err = accountRepo.Create(ctx, tenantID, domain.Account{
				ExternalAccountRef: "ACC-001",
				AccountType:        domain.AccountTypeBank,
				Currency:           "USD",
				Name:               "Main Bank Account",
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if account.ID == uuid.Nil {
			t.Fatal("expected a generated ID")
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			got, err := accountRepo.GetByID(ctx, tenantID, account.ID)
			if err != nil {
				return err
			}
			if got.Name != "Main Bank Account" || got.AccountType != domain.AccountTypeBank {
				t.Errorf("round-trip mismatch: got %+v", got)
			}
			list, err := accountRepo.ListByTenant(ctx, tenantID)
			if err != nil {
				return err
			}
			if len(list) != 1 {
				t.Errorf("expected 1 account, got %d", len(list))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("GetByID/ListByTenant failed: %v", err)
		}
	})

	t.Run("StatementRepo", func(t *testing.T) {
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			statement, err = statementRepo.Create(ctx, tenantID, domain.Statement{
				AccountID:      account.ID,
				Format:         "MT940",
				ReceivedAt:     time.Now(),
				PeriodStart:    time.Now().AddDate(0, 0, -1),
				PeriodEnd:      time.Now(),
				OpeningBalance: decimal.RequireFromString("1000.00"),
				ClosingBalance: decimal.RequireFromString("1500.00"),
				Status:         domain.StatementStatusReceived,
				Checksum:       "abc123",
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			if err := statementRepo.UpdateStatus(ctx, tenantID, statement.ID, domain.StatementStatusValidated); err != nil {
				return err
			}
			got, err := statementRepo.GetByID(ctx, tenantID, statement.ID)
			if err != nil {
				return err
			}
			if got.Status != domain.StatementStatusValidated {
				t.Errorf("expected status VALIDATED, got %s", got.Status)
			}
			exists, err := statementRepo.ExistsByChecksum(ctx, tenantID, account.ID, "abc123")
			if err != nil {
				return err
			}
			if !exists {
				t.Error("expected ExistsByChecksum to find the seeded checksum")
			}
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateStatus/GetByID/ExistsByChecksum failed: %v", err)
		}
	})

	t.Run("TransactionRepo", func(t *testing.T) {
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			transaction, err = txRepo.Create(ctx, tenantID, domain.Transaction{
				AccountID:               account.ID,
				StatementID:             &statement.ID,
				ExternalRef:             "TX-001",
				Amount:                  decimal.RequireFromString("250.5000"),
				Currency:                "USD",
				BaseAmount:              decimal.RequireFromString("250.5000"),
				ValueDate:               time.Now(),
				BookingDate:             time.Now(),
				Side:                    domain.TransactionSideCredit,
				SourceMode:              domain.SourceModeBatch,
				IngestionIdempotencyKey: uuid.NewString(),
				Status:                  domain.TransactionStatusUnmatched,
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		if !transaction.Amount.Equal(decimal.RequireFromString("250.5000")) {
			t.Errorf("decimal precision not preserved: got %s", transaction.Amount)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			got, err := txRepo.GetByID(ctx, tenantID, transaction.ID)
			if err != nil {
				return err
			}
			if !got.Amount.Equal(transaction.Amount) {
				t.Errorf("round-trip amount mismatch: got %s want %s", got.Amount, transaction.Amount)
			}

			exists, err := txRepo.ExistsByIdempotencyKey(ctx, tenantID, transaction.IngestionIdempotencyKey)
			if err != nil {
				return err
			}
			if !exists {
				t.Error("expected ExistsByIdempotencyKey to find the seeded key")
			}

			unmatched, err := txRepo.ListUnmatched(ctx, tenantID, account.ID, time.Now().AddDate(0, 0, -7), time.Now().AddDate(0, 0, 7))
			if err != nil {
				return err
			}
			found := false
			for _, u := range unmatched {
				if u.ID == transaction.ID {
					found = true
				}
			}
			if !found {
				t.Error("expected ListUnmatched to include the seeded transaction")
			}

			return txRepo.UpdateStatus(ctx, tenantID, transaction.ID, domain.TransactionStatusMatched)
		})
		if err != nil {
			t.Fatalf("GetByID/ExistsByIdempotencyKey/ListUnmatched/UpdateStatus failed: %v", err)
		}
	})

	t.Run("MatchRuleRepo", func(t *testing.T) {
		spec, _ := json.Marshal(map[string]any{"rules": []any{}})
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			rule, err = ruleRepo.Create(ctx, tenantID, domain.MatchRule{
				Name:               "Test Rule",
				Version:            1,
				Status:             domain.MatchRuleStatusDraft,
				RuleSpec:           spec,
				MatchType:          domain.MatchRuleTypeExact,
				SourceAccountID:    &account.ID,
				TargetAccountID:    &account.ID,
				Priority:           10,
				AutoMatchThreshold: decimal.RequireFromString("0.92"),
				CreatedBy:          "test-user",
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			approver := "approver-user"
			if err := ruleRepo.UpdateStatus(ctx, tenantID, rule.ID, domain.MatchRuleStatusActive, &approver); err != nil {
				return err
			}
			active, err := ruleRepo.ListActive(ctx, tenantID, account.ID, account.ID)
			if err != nil {
				return err
			}
			if len(active) != 1 || active[0].ID != rule.ID {
				t.Errorf("expected ListActive to return the activated rule, got %+v", active)
			}
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateStatus/ListActive failed: %v", err)
		}
	})

	t.Run("MatchResultRepo", func(t *testing.T) {
		var result domain.MatchResult
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			result, err = resultRepo.Create(ctx, tenantID,
				domain.MatchResult{
					RuleID:          &rule.ID,
					MatchType:       domain.MatchCardinalityOneToOne,
					ConfidenceScore: decimal.RequireFromString("0.980"),
					Status:          domain.MatchResultStatusAutoMatched,
				},
				[]domain.MatchResultLine{
					{TransactionID: transaction.ID, Side: domain.MatchResultLineSideSource, AllocatedAmount: transaction.Amount},
				},
			)
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			gotResult, gotLines, err := resultRepo.GetByID(ctx, tenantID, result.ID)
			if err != nil {
				return err
			}
			if len(gotLines) != 1 || gotLines[0].TransactionID != transaction.ID {
				t.Errorf("expected 1 line for transaction %s, got %+v", transaction.ID, gotLines)
			}
			if gotResult.Status != domain.MatchResultStatusAutoMatched {
				t.Errorf("expected AUTO_MATCHED, got %s", gotResult.Status)
			}

			list, err := resultRepo.ListByStatus(ctx, tenantID, domain.MatchResultStatusAutoMatched)
			if err != nil {
				return err
			}
			if len(list) != 1 {
				t.Errorf("expected 1 result, got %d", len(list))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("GetByID/ListByStatus failed: %v", err)
		}
	})

	t.Run("CaseRepo", func(t *testing.T) {
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			caseRow, err = caseRepo.Create(ctx, tenantID, domain.Case{
				AccountID: account.ID,
				BreakType: domain.BreakTypeUnmatched,
				Status:    domain.CaseStatusOpen,
				Priority:  "P2",
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			if _, err := caseRepo.AddComment(ctx, tenantID, domain.CaseComment{CaseID: caseRow.ID, Actor: "analyst-1", EventType: "comment"}); err != nil {
				return err
			}
			if _, err := caseRepo.AddAuditEvent(ctx, tenantID, domain.CaseAuditEvent{CaseID: caseRow.ID, Actor: "system", EventType: "assigned"}); err != nil {
				return err
			}
			if err := caseRepo.UpdateStatus(ctx, tenantID, caseRow.ID, domain.CaseStatusResolved); err != nil {
				return err
			}

			got, err := caseRepo.GetByID(ctx, tenantID, caseRow.ID)
			if err != nil {
				return err
			}
			if got.Status != domain.CaseStatusResolved {
				t.Errorf("expected RESOLVED, got %s", got.Status)
			}
			if got.ResolvedAt == nil {
				t.Error("expected resolved_at to be set when status becomes RESOLVED")
			}

			comments, err := caseRepo.ListComments(ctx, tenantID, caseRow.ID)
			if err != nil {
				return err
			}
			if len(comments) != 1 {
				t.Errorf("expected 1 comment, got %d", len(comments))
			}

			events, err := caseRepo.ListAuditEvents(ctx, tenantID, caseRow.ID)
			if err != nil {
				return err
			}
			if len(events) != 1 {
				t.Errorf("expected 1 audit event, got %d", len(events))
			}

			list, err := caseRepo.ListByStatus(ctx, tenantID, domain.CaseStatusResolved)
			if err != nil {
				return err
			}
			if len(list) != 1 {
				t.Errorf("expected 1 resolved case, got %d", len(list))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("case workflow failed: %v", err)
		}
	})

	t.Run("ConnectorRepo", func(t *testing.T) {
		err := postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			var err error
			connector, err = connectorRepo.Create(ctx, tenantID, domain.Connector{
				Type:   "sftp",
				Status: "ACTIVE",
			})
			return err
		})
		if err != nil {
			t.Fatalf("Create failed: %v", err)
		}

		err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
			cursor, _ := json.Marshal(map[string]any{"offset": 42})
			if err := connectorRepo.UpdateCursorState(ctx, tenantID, connector.ID, cursor, time.Now()); err != nil {
				return err
			}
			got, err := connectorRepo.GetByID(ctx, tenantID, connector.ID)
			if err != nil {
				return err
			}
			if got.LastRunAt == nil {
				t.Error("expected last_run_at to be set")
			}
			list, err := connectorRepo.ListByTenant(ctx, tenantID)
			if err != nil {
				return err
			}
			if len(list) != 1 {
				t.Errorf("expected 1 connector, got %d", len(list))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("UpdateCursorState/GetByID/ListByTenant failed: %v", err)
		}
	})
}

// TestTransactionRepo_BulkInsert proves the COPY-based bulk path handles
// a realistic large batch (plans/task/core/05 DoD: "10k+ synthetic
// transactions").
func TestTransactionRepo_BulkInsert(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tenantID := uuid.New()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'Acme', 'STANDARD', 'us-east', 'ACTIVE')`,
		tenantID,
	)
	if err != nil {
		t.Fatalf("seed tenant failed: %v", err)
	}
	accountID := uuid.New()
	_, err = db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-BULK', 'BANK', 'USD', 'Bulk Test Account')`,
		accountID, tenantID,
	)
	if err != nil {
		t.Fatalf("seed account failed: %v", err)
	}

	appPool := appRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	tenantCtx := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantID})
	txRepo := postgres.NewTransactionRepo()

	const n = 10_000
	txs := make([]domain.Transaction, n)
	for i := 0; i < n; i++ {
		txs[i] = domain.Transaction{
			AccountID:               accountID,
			ExternalRef:             fmt.Sprintf("BULK-%d", i),
			Amount:                  decimal.NewFromInt(int64(i)),
			Currency:                "USD",
			BaseAmount:              decimal.NewFromInt(int64(i)),
			ValueDate:               time.Now(),
			BookingDate:             time.Now(),
			Side:                    domain.TransactionSideCredit,
			SourceMode:              domain.SourceModeBatch,
			IngestionIdempotencyKey: uuid.NewString(),
			Status:                  domain.TransactionStatusUnmatched,
		}
	}

	var inserted int
	err = postgres.WithTx(tenantCtx, appPool, tenantID, func(ctx context.Context) error {
		var err error
		inserted, err = txRepo.BulkInsert(ctx, tenantID, txs)
		return err
	})
	if err != nil {
		t.Fatalf("BulkInsert failed: %v", err)
	}
	if inserted != n {
		t.Fatalf("expected %d rows inserted, got %d", n, inserted)
	}

	var count int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM transactions WHERE account_id = $1`, accountID).Scan(&count); err != nil {
		t.Fatalf("count query failed: %v", err)
	}
	if count != n {
		t.Fatalf("expected %d queryable rows, got %d", n, count)
	}
}

// TestRepository_CrossTenantReadBlocked proves the app-layer explicit
// tenantID parameter AND RLS combine correctly: a read for tenant B's
// account, attempted using tenant A's ID, must not return tenant B's
// data. This is the first task where both independent defense-in-depth
// layers are exercised together against real repository code.
func TestRepository_CrossTenantReadBlocked(t *testing.T) {
	if testing.Short() {
		t.Skip("requires Docker - skipped under -short; run make test-integration")
	}

	db := testutil.StartPostgres(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tenantA := uuid.New()
	tenantB := uuid.New()
	for _, id := range []uuid.UUID{tenantA, tenantB} {
		_, err := db.Pool.Exec(ctx,
			`INSERT INTO tenants (id, name, isolation_tier, region, status) VALUES ($1, 'T', 'STANDARD', 'us-east', 'ACTIVE')`,
			id,
		)
		if err != nil {
			t.Fatalf("seed tenant failed: %v", err)
		}
	}

	accountB := uuid.New()
	_, err := db.Pool.Exec(ctx,
		`INSERT INTO accounts (id, tenant_id, external_account_ref, account_type, currency, name) VALUES ($1, $2, 'ACC-B', 'BANK', 'USD', 'Tenant B Account')`,
		accountB, tenantB,
	)
	if err != nil {
		t.Fatalf("seed account failed: %v", err)
	}

	appPool := appRolePool(t, ctx, db.DSN)
	defer appPool.Close()

	accountRepo := postgres.NewAccountRepo()

	// Run as tenant A (RLS session var set to tenant A), but query for
	// tenant B's account ID. Both the WHERE id = $1 (no tenant_id filter
	// needed since RLS applies it) and RLS itself must combine to hide
	// tenant B's row from a tenant-A-scoped session.
	ctxA := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantA})
	err = postgres.WithTx(ctxA, appPool, tenantA, func(ctx context.Context) error {
		_, err := accountRepo.GetByID(ctx, tenantA, accountB)
		return err
	})
	if err != postgres.ErrNotFound {
		t.Fatalf("expected ErrNotFound reading tenant B's account as tenant A, got %v", err)
	}

	// Sanity check: the same account IS visible when queried as tenant B.
	ctxB := tenancy.WithTenant(ctx, tenancy.TenantContext{TenantID: tenantB})
	err = postgres.WithTx(ctxB, appPool, tenantB, func(ctx context.Context) error {
		got, err := accountRepo.GetByID(ctx, tenantB, accountB)
		if err != nil {
			return err
		}
		if got.ID != accountB {
			t.Errorf("expected to find tenant B's own account, got %+v", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected tenant B to see its own account, got error: %v", err)
	}
}

// TestRepository_TenantIDMismatchRejectedBeforeDB proves the defensive
// equality check from plans/task/core/05 Implementation Notes: calling a
// repository method with a tenantID argument that doesn't match
// tenancy.MustTenantFromContext(ctx).TenantID is rejected immediately,
// before any DB access is attempted (no transaction is even opened here -
// the mismatch is caught by requireTx's equality check first).
func TestRepository_TenantIDMismatchRejectedBeforeDB(t *testing.T) {
	tenantA := uuid.New()
	tenantB := uuid.New()

	ctx := tenancy.WithTenant(context.Background(), tenancy.TenantContext{TenantID: tenantA})

	accountRepo := postgres.NewAccountRepo()
	// Deliberately pass tenantB while ctx carries tenantA - and no
	// transaction/pool is involved at all, proving this is rejected
	// purely by the parameter-vs-context check, not by a DB round trip.
	_, err := accountRepo.GetByID(ctx, tenantB, uuid.New())
	if err == nil {
		t.Fatal("expected an error for mismatched tenantID, got nil")
	}
}
