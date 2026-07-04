package connector_test

import (
	"context"
	"testing"

	"github.com/koriebruh/Jengine/internal/ingestion/connector"
)

type noopConnector struct{}

func (noopConnector) Fetch(_ context.Context, _ connector.ConnectorConfig) (<-chan connector.RawRecord, error) {
	return nil, nil
}
func (noopConnector) Validate(connector.ConnectorConfig) error { return nil }
func (noopConnector) SupportsStreaming() bool                  { return false }
func (noopConnector) Checkpoint() (connector.Cursor, error)    { return connector.Cursor{}, nil }

var _ connector.SourceConnector = noopConnector{}

func TestRegistry_RegisterAndNew(t *testing.T) {
	r := connector.NewRegistry()
	err := r.Register("noop", func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return noopConnector{}, nil
	})
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}

	c, err := r.New("noop", connector.ConnectorConfig{})
	if err != nil {
		t.Fatalf("New failed: %v", err)
	}
	if c == nil {
		t.Fatal("expected a non-nil connector")
	}
}

func TestRegistry_DuplicateRegistrationRejected(t *testing.T) {
	r := connector.NewRegistry()
	ctor := func(cfg connector.ConnectorConfig) (connector.SourceConnector, error) {
		return noopConnector{}, nil
	}
	if err := r.Register("noop", ctor); err != nil {
		t.Fatalf("first Register failed: %v", err)
	}
	if err := r.Register("noop", ctor); err == nil {
		t.Fatal("expected duplicate registration to be rejected, got nil error")
	}
}

func TestRegistry_UnknownTypeErrors(t *testing.T) {
	r := connector.NewRegistry()
	_, err := r.New("does-not-exist", connector.ConnectorConfig{})
	if err == nil {
		t.Fatal("expected an error for an unregistered connector type")
	}
}
