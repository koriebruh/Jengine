// Package dedup implements pipeline stage 6 (Dedup/Idempotency,
// plans/task/core/09): idempotency key computation, a Redis bitset bloom
// filter fast-path, and the authoritative ingestion_dedup-table-backed
// Stage. See plans/docs/02-data-ingestion.md §3.4.
package dedup
