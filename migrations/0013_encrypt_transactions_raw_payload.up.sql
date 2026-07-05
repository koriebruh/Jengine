-- plans/task/core/23 §10.1: pgcrypto for specifically sensitive
-- columns, enumerated here rather than blanket-encrypting the schema.
--
-- transactions.raw_payload is the ONE column encrypted by this task:
-- plans/docs/03-canonical-data-model.md §4.1 describes it as retaining
-- "original unmapped data for drill-down/audit" - the full original
-- source record (a raw MT940 narrative, a payment-gateway webhook
-- body, etc.) before field-mapping/normalization strips it down to
-- canonical fields. It commonly carries counterparty PII the mapped
-- canonical fields don't (this task's own named example category).
--
-- Not chosen: counterparty_ref - despite being the design's other
-- named PII example, it's an ACTIVE MATCHING KEY (internal/matching/
-- core's blocking/scoring reads it directly for fuzzy string
-- comparison) - encrypting it at rest would make it inert for the
-- matching engine's own core function, which is a correctness
-- regression this task must not introduce for a compliance win.
--
-- Existing rows are reset to NULL rather than re-encrypted in place
-- with a migration-time key: golang-migrate runs as a separate
-- process/tool from the app and has no access to the app-level
-- encryption key (internal/storage/postgres.TransactionRepo's
-- RawPayloadKey, resolved the same way every other secret in this
-- codebase is - env var in dev, Vault path in production). Acceptable
-- here since raw_payload is drill-down/audit convenience data, not the
-- system of record (transactions' canonical columns are unaffected) -
-- were this a production cutover with real historical data, the
-- correct expand-contract move would be a backfill job re-encrypting
-- existing plaintext under the real key before this column-type
-- change, not a bare migration. Column is now nullable (was NOT NULL
-- DEFAULT '{}'::jsonb) - application code treats NULL/empty
-- indistinguishably from an empty payload.
ALTER TABLE transactions ALTER COLUMN raw_payload DROP DEFAULT;
ALTER TABLE transactions ALTER COLUMN raw_payload DROP NOT NULL;
ALTER TABLE transactions ALTER COLUMN raw_payload TYPE bytea
  USING NULL;
