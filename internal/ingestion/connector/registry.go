package connector

import (
	"fmt"
	"sync"
)

// Constructor builds a SourceConnector from its config.
type Constructor func(cfg ConnectorConfig) (SourceConnector, error)

// Registry maps a connector type name to its Constructor. Registration is
// explicit (callers register at their own wiring point, e.g.
// cmd/ingestion-gateway/main.go), not via package init() - avoids hidden
// init()-order coupling, consistent with plans/docs/16-development-workflow.md
// §16.3's manual-constructor-injection philosophy (plans/task/core/06
// Implementation Notes).
type Registry struct {
	mu   sync.RWMutex
	ctor map[string]Constructor
}

func NewRegistry() *Registry {
	return &Registry{ctor: make(map[string]Constructor)}
}

// Register adds ctor under connectorType. Returns an error if
// connectorType is already registered - duplicate registration is a
// programming error, not something to silently overwrite.
func (r *Registry) Register(connectorType string, ctor Constructor) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.ctor[connectorType]; exists {
		return fmt.Errorf("connector: type %q already registered", connectorType)
	}
	r.ctor[connectorType] = ctor
	return nil
}

// New constructs a SourceConnector for cfg.Type.
func (r *Registry) New(connectorType string, cfg ConnectorConfig) (SourceConnector, error) {
	r.mu.RLock()
	ctor, ok := r.ctor[connectorType]
	r.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("connector: no constructor registered for type %q", connectorType)
	}
	return ctor(cfg)
}
