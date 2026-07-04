package domain

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type AccountType string

const (
	AccountTypeBank    AccountType = "BANK"
	AccountTypeGL      AccountType = "GL"
	AccountTypeGateway AccountType = "GATEWAY"
	AccountTypeCash    AccountType = "CASH"
)

// Account mirrors the accounts table (plans/docs/03-canonical-data-model.md
// §4.1, migrations/0001_init_schema.up.sql).
type Account struct {
	ID                 uuid.UUID
	TenantID           uuid.UUID
	ExternalAccountRef string
	AccountType        AccountType
	Currency           string
	Name               string
	Metadata           json.RawMessage
	CreatedAt          time.Time
	UpdatedAt          time.Time
}
