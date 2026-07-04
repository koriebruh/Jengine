// Package pipeline implements the ingestion pipeline orchestrator
// (plans/docs/02-data-ingestion.md §3.2): Raw Fetch -> Format Parse ->
// Field Mapping -> Normalization -> Validation -> Dedup/Idempotency ->
// Canonicalization -> Persist + Emit Event. See plans/task/core/06.
package pipeline
