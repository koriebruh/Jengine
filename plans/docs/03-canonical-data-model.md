> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [02-data-ingestion.md](02-data-ingestion.md)

# 03 — Canonical Data Model

## 4.1 Core Entities

```
Tenant
  id (uuid, PK), name, isolation_tier, region, created_at, status

Account   (bank account / ledger book / GL account being reconciled)
  id (uuid, PK), tenant_id (FK)
  external_account_ref, account_type (BANK|GL|GATEWAY|CASH), currency, name, metadata (jsonb)

Statement  (batch of transactions from one source at one point in time)
  id (uuid, PK), tenant_id, account_id (FK)
  source_connector_id, format (MT940|BAI2|CSV|API|...), received_at
  period_start, period_end, opening_balance, closing_balance, status (RECEIVED|PARSED|VALIDATED|RECONCILED)
  raw_file_ref (object storage pointer), checksum

Transaction  (canonical normalized matchable unit)
  id (uuid, PK), tenant_id, account_id (FK), statement_id (FK, nullable for streaming-only)
  external_ref, amount (numeric(20,4)), currency, fx_rate_to_base (nullable), base_amount
  value_date, booking_date, description, counterparty_ref
  transaction_type, side (DEBIT|CREDIT), source_mode (BATCH|STREAM)
  ingestion_idempotency_key (unique), status (UNMATCHED|MATCHED|PARTIALLY_MATCHED|EXCEPTION)
  raw_payload (jsonb), created_at, updated_at

MatchRule  (tenant-configured, versioned — see 04-matching-engine.md §5.1 DSL)
  id (uuid, PK), tenant_id, name, version, status (DRAFT|ACTIVE|ARCHIVED)
  rule_spec (jsonb, compiled DSL AST), match_type (EXACT|TOLERANCE|FUZZY|COMPOSITE)
  source_account_id, target_account_id (or account_group refs), priority
  auto_match_threshold, created_by, approved_by, effective_from

MatchResult / MatchGroup
  id (uuid, PK), tenant_id, rule_id (FK), match_type (ONE_TO_ONE|ONE_TO_MANY|MANY_TO_ONE|MANY_TO_MANY)
  confidence_score (0.0-1.0), status (AUTO_MATCHED|SUGGESTED|CONFIRMED|REJECTED)
  matched_at, matched_by, amount_variance, date_variance

MatchResultLine  (join: transactions ↔ MatchResult, role)
  match_result_id (FK), transaction_id (FK), side (SOURCE|TARGET), allocated_amount

Break / Case  (unresolved exception requiring human workflow)
  id (uuid, PK), tenant_id, account_id, related_transaction_ids
  break_type (UNMATCHED|AMOUNT_MISMATCH|TIMING_DIFFERENCE|DUPLICATE|FX_VARIANCE|MISSING_COUNTERPARTY)
  root_cause_category (nullable), status (OPEN|ASSIGNED|IN_PROGRESS|PENDING_APPROVAL|RESOLVED|WRITTEN_OFF|ESCALATED)
  assigned_to, priority, sla_due_at, opened_at, resolved_at, amount_at_risk, currency
  temporal_workflow_id (FK to Temporal workflow)

CaseComment / CaseAuditEvent  (append-only, never updated/deleted)
  id, case_id (FK), actor, event_type, payload (jsonb), created_at

AuditEvent  (system-wide immutable trail — superset of case events)
  id (ULID, PK — time-sortable), tenant_id, actor_id, actor_type
  event_type, entity_type, entity_id, before_state (jsonb), after_state (jsonb)
  ip_address, request_id, occurred_at, hash_chain_prev (tamper-evidence, see 09-security-compliance.md §10.1)

Connector / ConnectorConfig
  id, tenant_id, type, config (jsonb, secrets via Vault path reference not inline), schedule, status, last_run_at, cursor_state
```

Relationships: `Tenant 1—N Account`, `Account 1—N Statement`, `Statement 1—N Transaction`, `MatchRule N—N Account`, `MatchResult 1—N MatchResultLine N—1 Transaction`, `Break N—N Transaction`, `Break 1—1 Temporal workflow`, `Case 1—N CaseComment/CaseAuditEvent`.

## 4.2 Multi-Currency & Multi-Format Normalization
- Every `Transaction` stores original `amount`/`currency` plus `base_amount` converted at ingestion (tenant-configured FX source: rate table or integrated provider connector); `fx_rate_to_base`/`fx_rate_date` stored for audit traceability.
- Rules can match on native currency OR base-currency-normalized amounts (configurable per rule — FX-variance breaks are sometimes legitimate recon targets, not errors to normalize away).
- All source formats parsed by format-specific connectors into the same canonical `Transaction` struct — normalization layer is the single place format differences are absorbed, keeping Matching Engine entirely format-agnostic. `raw_payload` retains original unmapped data for drill-down/audit.

---
Next: [04-matching-engine.md](04-matching-engine.md)
