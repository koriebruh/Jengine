> Part of the Jengine Reconciliation Engine design doc set. Index: [README.md](README.md) · Prev: [04-matching-engine.md](04-matching-engine.md)

# 05 — Exception / Case Management Workflow

## 6.1 Break Lifecycle (Temporal-orchestrated state machine)

```
OPEN → ASSIGNED → IN_PROGRESS → PENDING_APPROVAL → RESOLVED
  │        │            │              │              │
  │        │            ▼              ▼              ▼
  │        │        ESCALATED ──► (re-assigned, SLA clock adjusted)
  │        │                                            │
  └────────┴──────────────────────────────────► WRITTEN_OFF (requires approval)
                                                          │
                                                     REOPENED (new evidence)
```

Each Break/Case backed by a **Temporal workflow instance** (`temporal_workflow_id`), giving for free: durable SLA timers (survive restarts/deploys, no cron-polling fragility), human-in-the-loop via Signals (assign/comment/submit-for-approval), full deterministic replayable history (= tamper-evident audit trail by construction).

## 6.2 Auto-Assignment
- Configurable per tenant: round-robin, load-balanced (fewest open cases), skill/account-based routing (e.g. "FX breaks > $100k → senior FX team"), or root-cause→team mapping.
- Runs as Temporal Activity on `OPEN` transition, consults tenant-scoped versioned `TeamRoutingConfig`.
- Bulk actions: multi-select assign/comment/resolve, single audit event referencing batch-op id + affected case ids.

## 6.3 SLA Tracking & Escalation
- `sla_due_at` computed from tenant SLA policy (business-hour-aware, shared holiday/business-calendar service with matching's date windows).
- Temporal timer at pre-breach checkpoints (75% elapsed → warning; 100% → escalate + priority bump + `sla.breached` webhook).
- ClickHouse-backed SLA dashboards (aging buckets, breach rate by team/root-cause, MTTR) — explicit target to match/exceed ReconArt's workflow reporting strength.

## 6.4 Approval Workflows (Maker-Checker)
- Financially consequential actions (confirm low-confidence match, write off a break, approve rule change) require second different user (`maker != checker` enforced at workflow level, not UI convention).
- Modeled as Temporal **child workflow** (`ApprovalWorkflow`) blocking parent until Approve/Reject signal from authorized approver (RBAC-checked at signal-handling time, defense-in-depth). Configurable multi-level chains (e.g. write-offs > $1M require two approvals), automatic reminders.

## 6.5 Comment/Audit Trail
- Append-only `CaseComment` (rich text, attachments via object storage pointer, @mentions → notification).
- Separate `CaseAuditEvent` for system-generated structured transitions (searchable comments via OpenSearch; structured audit events queryable for compliance).
- Global `AuditEvent` table additionally captures hash-chained compliance-grade record of same events (see [09-security-compliance.md](09-security-compliance.md) §10.1) — case-level trail optimized for UX, global log optimized for tamper-evidence/retention.

## 6.6 Root-Cause Categorization
- Configurable, tenant-extensible taxonomy seeded with defaults: Timing Difference, Data Entry Error, Duplicate Transaction, FX Rate Variance, Missing Counterparty Statement, System Interface Failure, Fraud/Investigation, Fee/Charge Discrepancy, Unauthorized Transaction.
- Feeds reporting (which root causes drive break volume) and future rule-suggestion features (e.g. "80% of breaks tagged 'Timing Difference' would auto-match if date-window widened to 3 days — apply?").

---
Next: [06-streaming-architecture.md](06-streaming-architecture.md)
