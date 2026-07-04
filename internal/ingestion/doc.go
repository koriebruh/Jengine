// Package ingestion holds the quarantine sink and transactional-outbox
// event emitter that plans/task/core/06's pipeline plugs into. Format
// parsers live in internal/ingestion/parsers, the field-mapping DSL in
// internal/ingestion/mapping, connectors in internal/ingestion/connector,
// and the stage orchestrator in internal/ingestion/pipeline - this
// top-level package is for the two cross-cutting sinks (quarantine,
// outbox) that don't belong to any one of those.
package ingestion
