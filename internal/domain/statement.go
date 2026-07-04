package domain

import (
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

type StatementStatus string

const (
	StatementStatusReceived   StatementStatus = "RECEIVED"
	StatementStatusParsed     StatementStatus = "PARSED"
	StatementStatusValidated  StatementStatus = "VALIDATED"
	StatementStatusReconciled StatementStatus = "RECONCILED"
)

// Statement mirrors the statements table.
type Statement struct {
	ID                uuid.UUID
	TenantID          uuid.UUID
	AccountID         uuid.UUID
	SourceConnectorID *uuid.UUID
	Format            string
	ReceivedAt        time.Time
	PeriodStart       time.Time
	PeriodEnd         time.Time
	OpeningBalance    decimal.Decimal
	ClosingBalance    decimal.Decimal
	Status            StatementStatus
	RawFileRef        string
	Checksum          string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}
