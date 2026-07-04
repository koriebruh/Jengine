// Package testconnector is a trivial fake SourceConnector used to
// exercise the ingestion pipeline (plans/task/core/06) end-to-end without
// depending on plans/task/core/07's real connectors. Not for use outside
// tests.
package testconnector

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"

	"github.com/koriebruh/Jengine/internal/domain"
	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

// Connector emits Records on Fetch, one at a time, then closes the
// channel. FailValidate, when set, makes Validate return an error.
type Connector struct {
	Records      []connector.RawRecord
	Streaming    bool
	FailValidate bool
}

func New(records []connector.RawRecord) *Connector {
	return &Connector{Records: records}
}

func (c *Connector) Fetch(ctx context.Context, cfg connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	ch := make(chan connector.RawRecord)
	go func() {
		defer close(ch)
		for _, rec := range c.Records {
			select {
			case ch <- rec:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}

func (c *Connector) Validate(cfg connector.ConnectorConfig) error {
	if c.FailValidate {
		return context.DeadlineExceeded // any non-nil sentinel; content doesn't matter for tests
	}
	return nil
}

func (c *Connector) SupportsStreaming() bool {
	return c.Streaming
}

func (c *Connector) Checkpoint() (connector.Cursor, error) {
	state, _ := json.Marshal(map[string]any{"records_seen": len(c.Records)})
	return connector.Cursor{State: state, UpdatedAt: time.Now()}, nil
}

// NewRecord is a small helper for tests building synthetic RawRecords.
func NewRecord(tenantID, connectorID uuid.UUID, payload []byte) connector.RawRecord {
	return connector.RawRecord{
		TenantID:     tenantID,
		ConnectorID:  connectorID,
		SourceFormat: "TESTFORMAT",
		Payload:      payload,
		ReceivedAt:   time.Now(),
		BatchID:      uuid.New(),
		SourceMode:   domain.SourceModeBatch,
	}
}
