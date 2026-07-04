package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type TransactionSide string

const (
	TransactionSideDebit  TransactionSide = "DEBIT"
	TransactionSideCredit TransactionSide = "CREDIT"
)

type SourceMode string

const (
	SourceModeBatch  SourceMode = "BATCH"
	SourceModeStream SourceMode = "STREAM"
)

type TransactionStatus string

const (
	TransactionStatusUnmatched        TransactionStatus = "UNMATCHED"
	TransactionStatusMatched          TransactionStatus = "MATCHED"
	TransactionStatusPartiallyMatched TransactionStatus = "PARTIALLY_MATCHED"
	TransactionStatusException        TransactionStatus = "EXCEPTION"
)

// Transaction mirrors the transactions table - the atomic matchable unit.
// Amount/BaseAmount are decimal.Decimal, never float64 (plans/task/core/05
// Common Pitfalls - a hard-line rule in a reconciliation engine).
type Transaction struct {
	ID                      uuid.UUID
	TenantID                uuid.UUID
	AccountID               uuid.UUID
	StatementID             *uuid.UUID
	ExternalRef             string
	Amount                  decimal.Decimal
	Currency                string
	FXRateToBase            *decimal.Decimal
	BaseAmount              decimal.Decimal
	ValueDate               time.Time
	BookingDate             time.Time
	Description             string
	CounterpartyRef         string
	TransactionType         string
	Side                    TransactionSide
	SourceMode              SourceMode
	IngestionIdempotencyKey string
	Status                  TransactionStatus
	RawPayload              json.RawMessage
	CreatedAt               time.Time
	UpdatedAt               time.Time
}
